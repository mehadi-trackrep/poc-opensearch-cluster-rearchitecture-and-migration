"""
Step 2 – Register an S3 (MinIO) snapshot repository on SOURCE and take a snapshot.

Production:  set S3_ENDPOINT to your real AWS endpoint (or leave blank for AWS).
Local POC:   S3_ENDPOINT=http://localhost:9000  →  MinIO running in Docker.

The repository-s3 plugin is installed via Dockerfile on top of opensearch:1.3.14.
Credentials were injected into each node's keystore by keystore-entrypoint.sh
at startup; here we only pass the non-secret settings (bucket, endpoint, etc.).

Run:  uv run python -m scripts.step02_create_snapshot
"""

import os
import time

import click

from scripts.common import (
    console,
    print_index_stats,
    source_client,
    wait_for_green,
)


def _s3_repo_settings() -> dict:
    settings: dict = {
        "bucket":   os.getenv("S3_BUCKET",   "opensearch-snapshots"),
        "region":   os.getenv("S3_REGION",   "us-east-1"),
        # path_style_access=true is required for MinIO and on-prem S3
        "path_style_access": "true",
        "compress": "true",
    }
    endpoint = os.getenv("S3_ENDPOINT_INTERNAL") or os.getenv("S3_ENDPOINT", "")
    if endpoint:
        # Strip scheme — repository-s3 plugin expects just host[:port], protocol set separately
        settings["endpoint"] = endpoint.replace("http://", "").replace("https://", "")
        settings["protocol"] = "http" if "http://" in (os.getenv("S3_ENDPOINT_INTERNAL") or "") else "https"
    return settings


def _register_repo(client, repo_name: str) -> None:
    settings = _s3_repo_settings()
    client.snapshot.create_repository(
        repository=repo_name,
        body={"type": "s3", "settings": settings},
    )
    console.print(f"[green]✓ S3 repo '{repo_name}' registered[/green]")
    console.print(f"  bucket   : {settings['bucket']}")
    console.print(f"  endpoint : {settings.get('endpoint', 'AWS default')}")


def _wait_for_snapshot(client, repo_name: str, snapshot_name: str, timeout: int = 300) -> None:
    console.print("[yellow]Waiting for snapshot to complete...[/yellow]")
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = client.snapshot.get(repository=repo_name, snapshot=snapshot_name)
        state = info["snapshots"][0]["state"]
        if state == "SUCCESS":
            console.print(f"[green]✓ Snapshot '{snapshot_name}' completed[/green]")
            return
        if state in ("FAILED", "PARTIAL"):
            raise RuntimeError(f"Snapshot ended with state: {state}")
        time.sleep(5)
    raise TimeoutError("Snapshot did not complete in time")


@click.command()
@click.option("--repo",     default=lambda: os.getenv("SNAPSHOT_REPO_NAME", "s3-repo"))
@click.option("--snapshot", default=lambda: os.getenv("SNAPSHOT_NAME", "mmh-poc-snapshot"))
@click.option("--index",    default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
@click.option("--wait/--no-wait", default=True)
def main(repo: str, snapshot: str, index: str, wait: bool) -> None:
    client = source_client()
    wait_for_green(client, "source cluster")

    print_index_stats(client, index)
    _register_repo(client, repo)

    # Delete pre-existing snapshot (idempotent re-runs)
    try:
        client.snapshot.delete(repository=repo, snapshot=snapshot)
        console.print(f"[yellow]Deleted old snapshot '{snapshot}'[/yellow]")
    except Exception:
        pass

    client.snapshot.create(
        repository=repo,
        snapshot=snapshot,
        body={
            "indices":              index,
            "ignore_unavailable":   True,
            "include_global_state": False,
            "metadata":             {"created_by": "poc-script", "source_index": index},
        },
        params={"wait_for_completion": "false"},
    )
    console.print(f"[green]✓ Snapshot '{snapshot}' started[/green]")

    if wait:
        _wait_for_snapshot(client, repo, snapshot)

    info = client.snapshot.get(repository=repo, snapshot=snapshot)["snapshots"][0]
    console.print(f"  state      : {info['state']}")
    console.print(f"  indices    : {info['indices']}")
    console.print(f"  shards     : {info['shards']}")
    console.print(f"  start_time : {info['start_time']}")
    console.print(f"  end_time   : {info.get('end_time', 'in progress')}")
    console.print(
        f"\n[dim]Snapshot is stored in MinIO bucket '{os.getenv('S3_BUCKET', 'opensearch-snapshots')}'[/dim]"
    )
    console.print("[dim]Browse it at http://localhost:9001  (minioadmin / minioadmin)[/dim]")


if __name__ == "__main__":
    main()
