"""
Step 5 – Remote _reindex: pull delta documents from SOURCE → TARGET.

Delta = documents whose `indexed_at` date is AFTER the snapshot was taken.
Pass --since as an ISO-8601 timestamp (e.g. "2026-05-06T10:00:00Z").
If omitted, the script fetches the snapshot creation time automatically from
the snapshot metadata so you don't have to look it up manually.

The target nodes already have `reindex.remote.whitelist` pointing at the source
cluster (set in docker-compose.yml), so no extra config is needed.

Run:
    # auto-detect snapshot time
    uv run python -m scripts.step05_remote_reindex_for_delta

    # or pass an explicit cutoff
    uv run python -m scripts.step05_remote_reindex_for_delta --since 2026-05-06T10:00:00Z
"""

import os
import time

import click

from scripts.common import (
    console,
    print_index_stats,
    source_client,
    target_client,
    wait_for_green,
)


def _get_snapshot_end_time(src_client, repo: str, snapshot: str) -> str:
    """Return the snapshot's end_time so we know the exact cutoff for delta."""
    info = src_client.snapshot.get(repository=repo, snapshot=snapshot)
    snap = info["snapshots"][0]
    end_time = snap.get("end_time") or snap.get("start_time")
    console.print(f"[dim]Snapshot '{snapshot}' end_time: {end_time}[/dim]")
    return end_time


def _wait_for_task(client, task_id: str, timeout: int = 600) -> dict:
    console.print(f"[yellow]Remote reindex task {task_id} running...[/yellow]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = client.tasks.get(task_id=task_id)
        if result.get("completed"):
            return result
        progress = result["task"].get("status", {})
        total   = progress.get("total", 0)
        created = progress.get("created", 0)
        if total:
            console.print(f"  {created}/{total} docs ({100*created//total}%)", end="\r")
        time.sleep(5)
    raise TimeoutError("Remote reindex task did not complete in time")


@click.command()
@click.option("--source-index", default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
@click.option("--target-index", default=lambda: os.getenv("TARGET_INDEX_REINDEXED", "mmh-poc-v2"))
@click.option(
    "--source-host",
    default="http://src-data-1:9200",
    help="Internal Docker address used by TARGET nodes to reach SOURCE (not the host port).",
    show_default=True,
)
@click.option(
    "--since",
    default=None,
    help="ISO-8601 cutoff timestamp. Docs with indexed_at > this value are pulled. "
         "Defaults to the snapshot end_time fetched automatically.",
)
@click.option("--repo",     default=lambda: os.getenv("SNAPSHOT_REPO_NAME", "s3-repo"))
@click.option("--snapshot", default=lambda: os.getenv("SNAPSHOT_NAME", "mmh-poc-snapshot"))
def main(
    source_index: str,
    target_index: str,
    source_host: str,
    since: str | None,
    repo: str,
    snapshot: str,
) -> None:
    src_client = source_client()
    tgt_client = target_client()

    wait_for_green(src_client, "source cluster")
    wait_for_green(tgt_client, "target cluster")

    # ── Determine delta cutoff timestamp ─────────────────────────────────────
    if since is None:
        since = _get_snapshot_end_time(src_client, repo, snapshot)

    console.print(f"\n[bold]Delta cutoff:[/bold] indexed_at > [cyan]{since}[/cyan]")

    delta_query = {
        "range": {
            "indexed_at": {
                "gt": since   # strictly AFTER snapshot → only new/updated docs
            }
        }
    }

    # ── Count delta docs on source ────────────────────────────────────────────
    delta_count = src_client.count(
        index=source_index, body={"query": delta_query}
    )["count"]

    console.print(
        f"[bold]{delta_count}[/bold] delta docs found in source "
        f"'{source_index}' (indexed_at > {since})"
    )

    if delta_count == 0:
        console.print("[yellow]No delta docs — nothing to pull.[/yellow]")
        return

    before = tgt_client.count(index=target_index)["count"]
    console.print(f"Target '{target_index}' currently has [bold]{before}[/bold] docs")

    # ── Remote reindex: target pulls from source ──────────────────────────────
    resp = tgt_client.reindex(
        body={
            "source": {
                "remote": {
                    "host": source_host,
                    # no auth — DISABLE_SECURITY_PLUGIN=true on source nodes
                },
                "index": source_index,
                "size":  500,
                "query": delta_query,
            },
            "dest": {
                "index":   target_index,
                "op_type": "index",   # upsert — safe to re-run
            },
            # Strip contacts_designation_labels_2 so strict mapping on target
            # doesn't reject docs that still carry the old field.
            "script": {
                "lang":   "painless",
                "source": "ctx._source.remove('contacts_designation_labels_2');",
            },
        },
        params={"wait_for_completion": "false", "refresh": "true"},
    )
    task_id = resp["task"]
    console.print(f"[green]✓ Remote reindex started (task_id={task_id})[/green]")

    result   = _wait_for_task(tgt_client, task_id)
    status   = result.get("response") or result["task"]["status"]
    created  = status.get("created", 0)
    updated  = status.get("updated", 0)
    failures = status.get("failures", [])

    console.print(f"\n[bold]Remote reindex complete[/bold]")
    console.print(f"  created  : {created}")
    console.print(f"  updated  : {updated}")
    console.print(f"  failures : {len(failures)}")
    if failures:
        for f in failures[:5]:
            console.print(f"  [red]{f}[/red]")

    tgt_client.indices.refresh(index=target_index)
    after = tgt_client.count(index=target_index)["count"]
    console.print(f"\nTarget '{target_index}' now has [bold]{after}[/bold] docs (was {before})")

    src_total = src_client.count(index=source_index)["count"]
    console.print(f"\n{'─'*50}")
    console.print(f"Source total  : {src_total}")
    console.print(f"Target total  : {after}")
    if src_total == after:
        console.print("[green bold]✓ Counts match — migration complete![/green bold]")
    else:
        console.print(f"[yellow]Difference: {src_total - after} docs[/yellow]")

    print_index_stats(src_client, source_index)
    print_index_stats(tgt_client, target_index)


if __name__ == "__main__":
    main()
