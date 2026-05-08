"""
Step 4 – Reindex `mmh-poc-restored` → `mmh-poc-v2` on TARGET with new shard settings.

Creates `mmh-poc-v2` with the desired shard/replica count (from .env), runs
`_reindex`, verifies doc counts match, then creates an alias `mmh-poc` pointing
to `mmh-poc-v2` so the application doesn't need to change its index name.

Run:  uv run python -m scripts.step04_reindex_target
"""

import os
import time

import click

from scripts.common import (
    console,
    print_index_stats,
    target_client,
    wait_for_green,
)

SOURCE_SUFFIX = "-restored"


DROP_FIELDS = {"contacts_designation_labels_2"}


def _build_new_mapping(client, source_index: str) -> dict:
    """Copy mappings from source_index, drop deprecated fields, apply new shard settings."""
    mapping = client.indices.get_mapping(index=source_index)[source_index]
    settings = client.indices.get_settings(index=source_index)[source_index]["settings"]

    # Remove fields that should not exist in the new index
    props = mapping["mappings"].get("properties", {})
    for field in DROP_FIELDS:
        if field in props:
            del props[field]
            console.print(f"[yellow]Dropped field '{field}' from mmh-poc-v2 mapping[/yellow]")

    return {
        "settings": {
            "number_of_shards":   int(os.getenv("TARGET_SHARDS", "30")),
            "number_of_replicas": int(os.getenv("TARGET_REPLICAS", "1")),
            "refresh_interval":   "1s",
            # carry over any custom analyzers / filters
            "analysis": settings.get("analysis", {}),
        },
        "mappings": mapping["mappings"],
    }


def _wait_for_task(client, task_id: str, timeout: int = 600) -> dict:
    console.print(f"[yellow]Reindex task {task_id} running...[/yellow]")
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
    raise TimeoutError("Reindex task did not complete in time")


@click.command()
@click.option("--source-index", default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
@click.option("--target-index", default=lambda: os.getenv("TARGET_INDEX_REINDEXED", "mmh-poc-v2"))
@click.option("--alias",        default="mmh-poc", show_default=True)
@click.option("--shards",   default=lambda: int(os.getenv("TARGET_SHARDS",   "30")), type=int)
@click.option("--replicas", default=lambda: int(os.getenv("TARGET_REPLICAS", "1")),  type=int)
def main(source_index: str, target_index: str, alias: str, shards: int, replicas: int) -> None:
    restored_index = f"{source_index}{SOURCE_SUFFIX}"
    client = target_client()

    wait_for_green(client, "target cluster")

    if not client.indices.exists(index=restored_index):
        console.print(f"[red]'{restored_index}' not found — run step03 first[/red]")
        raise SystemExit(1)

    src_count = client.count(index=restored_index)["count"]
    console.print(f"Source '{restored_index}' has [bold]{src_count}[/bold] documents")

    # Create target index with new shard layout
    if client.indices.exists(index=target_index):
        client.indices.delete(index=target_index)
        console.print(f"[yellow]Dropped old '{target_index}'[/yellow]")

    body = _build_new_mapping(client, restored_index)
    body["settings"]["number_of_shards"]   = shards
    body["settings"]["number_of_replicas"] = replicas
    client.indices.create(index=target_index, body=body)
    console.print(
        f"[green]✓ Created '{target_index}' with {shards} shards / {replicas} replicas[/green]"
    )

    # Build Painless script to remove dropped fields so strict mapping doesn't reject docs
    remove_stmts = " ".join(f"ctx._source.remove('{f}');" for f in DROP_FIELDS)

    # Kick off async reindex
    resp = client.reindex(
        body={
            "source": {"index": restored_index, "size": 1000},
            "dest":   {"index": target_index,   "op_type": "index"},
            "script": {
                "lang":   "painless",
                "source": remove_stmts,
            },
        },
        params={"wait_for_completion": "false", "refresh": "true"},
    )
    task_id = resp["task"]
    console.print(f"[green]✓ Reindex started (task_id={task_id})[/green]")

    result = _wait_for_task(client, task_id)
    status = result["response"] if "response" in result else result["task"]["status"]
    created  = status.get("created", 0)
    updated  = status.get("updated", 0)
    failures = status.get("failures", [])

    console.print(f"\n[bold]Reindex complete[/bold]")
    console.print(f"  created  : {created}")
    console.print(f"  updated  : {updated}")
    console.print(f"  failures : {len(failures)}")

    # Verify counts
    client.indices.refresh(index=target_index)
    dst_count = client.count(index=target_index)["count"]
    if dst_count == src_count:
        console.print(f"[green]✓ Doc count matches: {dst_count}[/green]")
    else:
        console.print(f"[red]⚠ Count mismatch: source={src_count}, target={dst_count}[/red]")

    # Create alias
    try:
        client.indices.delete_alias(index="*", name=alias)
    except Exception:
        pass
    client.indices.put_alias(index=target_index, name=alias)
    console.print(f"[green]✓ Alias '{alias}' → '{target_index}'[/green]")

    print_index_stats(client, target_index)
    console.print("\n[dim]Next: run step05_add_delta to add new docs to source, then step06[/dim]")


if __name__ == "__main__":
    main()
