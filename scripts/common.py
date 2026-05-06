"""Shared helpers: client factory, wait-for-green, pretty printing."""

import os
import time

from dotenv import load_dotenv
from opensearchpy import OpenSearch
from rich.console import Console
from rich.table import Table

load_dotenv()

console = Console()

# Security is disabled on both clusters (DISABLE_SECURITY_PLUGIN=true in compose).
# Set SECURITY_ENABLED=true only if you re-enable the security plugin.
_SECURITY_ENABLED = os.getenv("SECURITY_ENABLED", "false").lower() == "true"


def get_client(host: str, port: int, user: str = "", password: str = "") -> OpenSearch:
    kwargs = dict(
        hosts=[{"host": host, "port": port}],
        use_ssl=False,
        verify_certs=False,
        ssl_show_warn=False,
        timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )
    if _SECURITY_ENABLED and user:
        kwargs["http_auth"] = (user, password)
    return OpenSearch(**kwargs)


def source_client() -> OpenSearch:
    return get_client(
        host=os.getenv("SOURCE_HOST", "localhost"),
        port=int(os.getenv("SOURCE_PORT", "9200")),
        user=os.getenv("SOURCE_USER", "admin"),
        password=os.getenv("SOURCE_PASS", "admin"),
    )


def target_client() -> OpenSearch:
    return get_client(
        host=os.getenv("TARGET_HOST", "localhost"),
        port=int(os.getenv("TARGET_PORT", "9201")),
        user=os.getenv("TARGET_USER", "admin"),
        password=os.getenv("TARGET_PASS", "admin"),
    )


def wait_for_green(client: OpenSearch, label: str, timeout: int = 180) -> None:
    console.print(f"[yellow]Waiting for {label} to be green...[/yellow]")
    last_error = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            health = client.cluster.health()
            status = health["status"]
            if status == "green":
                console.print(f"[green]✓ {label} is green[/green]")
                return
            console.print(f"  {label}: {status} — waiting...", end="\r")
        except Exception as exc:
            last_error = exc
            console.print(f"  [dim]{label}: {exc}[/dim]", end="\r")
        time.sleep(5)
    raise TimeoutError(
        f"{label} did not reach green within {timeout}s. Last error: {last_error}"
    )


def wait_for_yellow(client: OpenSearch, label: str, timeout: int = 180) -> None:
    console.print(f"[yellow]Waiting for {label} to be available...[/yellow]")
    last_error = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            health = client.cluster.health()
            status = health["status"]
            if status in ("yellow", "green"):
                console.print(f"[green]✓ {label} is available ({status})[/green]")
                return
            console.print(f"  {label}: {status} — waiting...", end="\r")
        except Exception as exc:
            last_error = exc
            console.print(f"  [dim]{label}: {exc}[/dim]", end="\r")
        time.sleep(5)
    raise TimeoutError(
        f"{label} did not become available within {timeout}s. Last error: {last_error}"
    )


def print_cluster_info(client: OpenSearch, label: str) -> None:
    health = client.cluster.health()
    info = client.info()
    t = Table(title=f"{label} Cluster Info")
    t.add_column("Key", style="cyan")
    t.add_column("Value", style="white")
    t.add_row("Name", health["cluster_name"])
    t.add_row("Status", f"[green]{health['status']}[/green]")
    t.add_row("Nodes", str(health["number_of_nodes"]))
    t.add_row("Data nodes", str(health["number_of_data_nodes"]))
    t.add_row("Version", info["version"]["number"])
    t.add_row("Active shards", str(health["active_shards"]))
    console.print(t)


def print_index_stats(client: OpenSearch, index: str) -> None:
    try:
        stats = client.indices.stats(index=index)
        count = client.count(index=index)["count"]
        settings = client.indices.get_settings(index=index)[index]["settings"]["index"]
        t = Table(title=f"Index: {index}")
        t.add_column("Key", style="cyan")
        t.add_column("Value", style="white")
        t.add_row("Doc count", str(count))
        t.add_row("Primary shards", settings.get("number_of_shards", "?"))
        t.add_row("Replicas", settings.get("number_of_replicas", "?"))
        size = stats["_all"]["primaries"]["store"]["size_in_bytes"]
        t.add_row("Size (primaries)", f"{size / 1024:.1f} KB")
        console.print(t)
    except Exception as exc:
        console.print(f"[red]Could not fetch stats for {index}: {exc}[/red]")
