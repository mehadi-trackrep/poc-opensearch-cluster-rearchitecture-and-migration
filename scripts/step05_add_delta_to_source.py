"""
Utility – Bulk-index N delta documents into the SOURCE cluster.

Simulates live company data arriving AFTER the snapshot was taken.
Every document gets `indexed_at = now()` so the timestamp is guaranteed
to be later than the snapshot end_time, making them visible to the
range query in step05_remote_reindex_for_delta.

Run:
    uv run python -m scripts.add_delta_to_source            # 200 docs
    uv run python -m scripts.add_delta_to_source --count 500
"""

import os
import random
from datetime import datetime, timedelta, timezone

import click
from faker import Faker
from opensearchpy.helpers import bulk
from rich.progress import track

from scripts.common import (
    console,
    print_index_stats,
    source_client,
    wait_for_green,
)
from scripts.step01_setup_source import (
    COUNTRY_CODES,
    COUNTRY_WEIGHTS,
    _random_designations,
    _random_industries,
)

fake = Faker()


def _random_created() -> str:
    delta = random.randint(0, 1825)
    dt = datetime.now(timezone.utc) - timedelta(days=delta)
    return dt.isoformat()


def _generate_delta_doc(index: str, indexed_at: str) -> dict:
    orgno = random.randint(100_000, 999_999_999)
    country = random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS, k=1)[0]
    return {
        "_index":   index,
        "_id":      str(orgno),
        "_routing": str(orgno),
        "_source": {
            "orgno":         orgno,
            "company_name":  fake.company(),
            "contacts_designation_labels_2":  _random_designations(),
            "contacts_designation_labels_v2": _random_designations(),
            "country_code":   country,
            "created_time":   _random_created(),
            "indexed_at":     indexed_at,
            "industry_labels": _random_industries(),
        },
    }


@click.command()
@click.option("--count", default=200, show_default=True, help="Number of delta docs to add")
@click.option("--index", default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
def main(count: int, index: str) -> None:
    client = source_client()
    wait_for_green(client, "source cluster")

    if not client.indices.exists(index=index):
        console.print(f"[red]Index '{index}' not found — run step01 first[/red]")
        raise SystemExit(1)

    before = client.count(index=index)["count"]
    console.print(f"Source '{index}' currently has [bold]{before}[/bold] docs")

    # All delta docs share the same indexed_at = now so they're easy to query
    indexed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    console.print(f"Delta [bold cyan]indexed_at[/bold cyan] = {indexed_at}")

    actions = (_generate_delta_doc(index, indexed_at) for _ in range(count))
    successes, errors = bulk(
        client,
        track(actions, description=f"Indexing {count} delta docs...", total=count),
        chunk_size=500,
    )

    client.indices.refresh(index=index)
    after = client.count(index=index)["count"]

    console.print(f"[green]✓ {successes} docs indexed  ({errors} errors)[/green]")
    console.print(
        f"Source '{index}': [bold]{before}[/bold] → [bold]{after}[/bold] docs  "
        f"(+{after - before} net, {count - successes} overwrote existing orgno)"
    )
    print_index_stats(client, index)
    console.print(
        f"\n[dim]Next: run step05 to pull these delta docs (indexed_at > snapshot end_time) "
        f"into the target cluster.[/dim]"
    )


if __name__ == "__main__":
    main()
