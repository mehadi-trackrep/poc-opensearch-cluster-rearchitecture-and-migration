"""
Step 5 – Simulate delta: add new documents to SOURCE after the snapshot.

These documents represent data written to the source cluster while the
migration was in progress.  Step 6 will use remote _reindex to pull only
these docs into the target.

The delta docs get IDs starting at 1_000_000 so they don't collide with
the seed data and are easy to filter with a range query.

Run:  uv run python -m scripts.step05_add_delta
"""

import os
from datetime import datetime, timezone

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

fake = Faker()

DELTA_ID_START = 1_000_000
STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
PRODUCTS = ["laptop", "phone", "tablet", "headphones", "monitor", "keyboard", "mouse"]


def _delta_order(i: int) -> dict:
    return {
        "_index": os.getenv("SOURCE_INDEX", "orders"),
        "_id": f"order-{i:07d}",
        "_source": {
            "order_id":      f"ORD-{i:07d}",
            "customer_id":   fake.uuid4(),
            "customer_name": fake.name(),
            "email":         fake.email(),
            "product":       fake.random_element(PRODUCTS),
            "amount":        round(fake.pyfloat(min_value=5, max_value=2000, right_digits=2), 2),
            "currency":      "USD",
            "status":        fake.random_element(STATUSES),
            "created_at":    datetime.now(timezone.utc).isoformat(),
            "updated_at":    datetime.now(timezone.utc).isoformat(),
            "tags":          ["delta", "post-snapshot"],
            "notes":         fake.sentence(),
        },
    }


@click.command()
@click.option("--count", default=100, show_default=True, help="Number of delta docs to add")
def main(count: int) -> None:
    index = os.getenv("SOURCE_INDEX", "orders")
    client = source_client()

    wait_for_green(client, "source cluster")

    before = client.count(index=index)["count"]
    console.print(f"Source '{index}' currently has [bold]{before}[/bold] documents")

    actions = (_delta_order(DELTA_ID_START + i) for i in range(count))
    successes, errors = bulk(
        client, track(actions, description="Adding delta docs...", total=count)
    )
    client.indices.refresh(index=index)

    after = client.count(index=index)["count"]
    console.print(f"[green]✓ Added {successes} delta docs ({errors} errors)[/green]")
    console.print(f"Source now has [bold]{after}[/bold] documents (delta={after - before})")

    # Show a sample delta doc ID for reference
    console.print(f"\n[dim]Delta docs have IDs order-{DELTA_ID_START:07d} .. order-{DELTA_ID_START+count-1:07d}[/dim]")
    console.print(f"[dim]Filter hint:  'order_id' >= 'ORD-{DELTA_ID_START:07d}'[/dim]")

    print_index_stats(client, index)


if __name__ == "__main__":
    main()
