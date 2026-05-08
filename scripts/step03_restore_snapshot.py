"""
Step 3 – Register the S3 (MinIO) repo on TARGET and restore the snapshot.

Both clusters point at the same MinIO bucket; TARGET registers it as readonly
so it can never accidentally overwrite a snapshot.

The restored index is renamed to `mmh-poc-restored` so Step 4 can reindex it
into `mmh-poc-v2` with new shard settings without touching the original name.

Run:  uv run python -m scripts.step03_restore_snapshot
"""

import os
import time

import click

from scripts.common import (
    console,
    print_cluster_info,
    print_index_stats,
    target_client,
    wait_for_green,
)

RESTORED_SUFFIX = "-restored"


def _s3_repo_settings(readonly: bool = True) -> dict:
    settings: dict = {
        "bucket":            os.getenv("S3_BUCKET",  "opensearch-snapshots"),
        "region":            os.getenv("S3_REGION",  "us-east-1"),
        "path_style_access": "true",
        "compress":          "true",
        "readonly":          str(readonly).lower(),
    }
    endpoint = os.getenv("S3_ENDPOINT_INTERNAL") or os.getenv("S3_ENDPOINT", "")
    if endpoint:
        settings["endpoint"] = endpoint.replace("http://", "").replace("https://", "")
        settings["protocol"] = "http" if "http://" in (os.getenv("S3_ENDPOINT_INTERNAL") or "") else "https"
    return settings


def _wait_for_restore(client, index: str, timeout: int = 300) -> None:
    console.print(f"[yellow]Waiting for restore of '{index}'...[/yellow]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            recovery = client.indices.recovery(index=index)
            shards = recovery.get(index, {}).get("shards", [])
            if shards and all(s["stage"] == "DONE" for s in shards):
                console.print(f"[green]✓ Restore complete[/green]")
                return
        except Exception:
            pass
        time.sleep(5)
    raise TimeoutError(f"Restore of '{index}' did not finish in {timeout}s")


@click.command()
@click.option("--repo",     default=lambda: os.getenv("SNAPSHOT_REPO_NAME", "s3-repo"))
@click.option("--snapshot", default=lambda: os.getenv("SNAPSHOT_NAME", "mmh-poc-snapshot"))
@click.option("--index",    default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
def main(repo: str, snapshot: str, index: str) -> None:
    restored_index = f"{index}{RESTORED_SUFFIX}"
    client = target_client()

    wait_for_green(client, "target cluster")
    print_cluster_info(client, "TARGET")

    # Register the same S3 bucket as readonly on the target
    client.snapshot.create_repository(
        repository=repo,
        body={"type": "s3", "settings": _s3_repo_settings(readonly=True)},
    )
    console.print(f"[green]✓ S3 repo '{repo}' registered on target (readonly)[/green]")

    # Verify snapshot exists and succeeded
    snap_info = client.snapshot.get(repository=repo, snapshot=snapshot)
    state = snap_info["snapshots"][0]["state"]
    if state != "SUCCESS":
        raise RuntimeError(f"Snapshot state is '{state}', expected SUCCESS")
    console.print(f"[green]✓ Snapshot '{snapshot}' visible from target — state: SUCCESS[/green]")

    # Drop pre-existing restored index
    if client.indices.exists(index=restored_index):
        client.indices.delete(index=restored_index)
        console.print(f"[yellow]Dropped existing '{restored_index}'[/yellow]")

    client.snapshot.restore(
        repository=repo,
        snapshot=snapshot,
        body={
            "indices":              index,
            "ignore_unavailable":   True,
            "include_global_state": False,
            # Rename so it doesn't collide with the live index named 'mmh-poc'
            "rename_pattern":     index,
            "rename_replacement": restored_index,
            "index_settings": {
                "index.number_of_replicas": 1,
            },
            "ignore_index_settings": ["index.refresh_interval"],
        },
        params={"wait_for_completion": "false"},
    )
    console.print(f"[green]✓ Restore started: '{snapshot}' → '{restored_index}'[/green]")

    _wait_for_restore(client, restored_index)
    print_index_stats(client, restored_index)

    doc_count = client.count(index=restored_index)["count"]
    console.print(f"\n[bold]Restored {doc_count} docs into '{restored_index}'[/bold]")
    console.print("[dim]Next: step04 — reindex into mmh-poc-v2 with new shard config[/dim]")


if __name__ == "__main__":
    main()
