"""
Step 6 – Remote _reindex: pull delta documents from SOURCE into TARGET.

Uses the `reindex.remote.whitelist` already set in target nodes
(docker-compose.yml) to pull docs directly from the source cluster.

The query filters to only the delta documents (order IDs >= ORD-1000000),
so we don't duplicate data already restored from the snapshot.

Run:  uv run python -m scripts.step06_remote_reindex
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

DELTA_ID_PREFIX = "ORD-1"   # all delta order_ids start with ORD-1xxxxxxx


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
@click.option("--source-index", default=lambda: os.getenv("SOURCE_INDEX", "orders"))
@click.option("--target-index", default=lambda: os.getenv("TARGET_INDEX_REINDEXED", "orders-v2"))
@click.option(
    "--source-host",
    default=lambda: f"http://src-data-1:9200",
    help="Internal Docker address of source cluster (used by target nodes for remote reindex)",
    show_default=True,
)
def main(source_index: str, target_index: str, source_host: str) -> None:
    src_client = source_client()
    tgt_client = target_client()

    wait_for_green(src_client, "source cluster")
    wait_for_green(tgt_client, "target cluster")

    # Count delta docs in source
    delta_query = {
        "query": {
            "prefix": {"order_id": DELTA_ID_PREFIX}
        }
    }
    delta_count = src_client.count(index=source_index, body=delta_query)["count"]
    console.print(f"[bold]{delta_count}[/bold] delta documents found in source '{source_index}'")

    if delta_count == 0:
        console.print("[yellow]No delta docs found — run step05 first[/yellow]")
        return

    before = tgt_client.count(index=target_index)["count"]
    console.print(f"Target '{target_index}' currently has [bold]{before}[/bold] documents")

    # Remote reindex: target pulls from source
    resp = tgt_client.reindex(
        body={
            "source": {
                "remote": {
                    "host": source_host,
                    # no auth needed because DISABLE_SECURITY_PLUGIN=true
                },
                "index": source_index,
                "size":  500,
                "query": delta_query["query"],
            },
            "dest": {
                "index":   target_index,
                "op_type": "index",  # upsert semantics — safe to re-run
            },
        },
        params={"wait_for_completion": "false", "refresh": "true"},
    )
    task_id = resp["task"]
    console.print(f"[green]✓ Remote reindex started (task_id={task_id})[/green]")

    result = _wait_for_task(tgt_client, task_id)
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

    # Final reconciliation
    src_total = src_client.count(index=source_index)["count"]
    console.print(f"\n{'─'*50}")
    console.print(f"Source total   : {src_total}")
    console.print(f"Target total   : {after}")
    if src_total == after:
        console.print("[green bold]✓ Counts match — migration complete![/green bold]")
    else:
        diff = src_total - after
        console.print(f"[yellow]Difference: {diff} docs — check for gaps[/yellow]")

    print_index_stats(src_client, source_index)
    print_index_stats(tgt_client, target_index)


if __name__ == "__main__":
    main()
