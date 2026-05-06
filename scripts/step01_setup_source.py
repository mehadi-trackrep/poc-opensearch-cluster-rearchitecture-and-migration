"""
Step 1 – Create mmh-poc on source cluster and bulk-load 5 000 mock documents.

Mapping is exact production mapping (dynamic:strict, _routing required).
Routing key = orgno (organisation number) — required on every operation.

Run:  uv run python -m scripts.step01_setup_source
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
    print_cluster_info,
    print_index_stats,
    source_client,
    wait_for_green,
)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── exact production mapping ──────────────────────────────────────────────────
_SOURCE_SHARDS   = int(os.getenv("SOURCE_SHARDS",   "5"))
_SOURCE_REPLICAS = int(os.getenv("SOURCE_REPLICAS", "1"))

INDEX_BODY = {
    "settings": {
        "number_of_shards":   _SOURCE_SHARDS,
        "number_of_replicas": _SOURCE_REPLICAS,
        "refresh_interval":   "5s",
    },
    "mappings": {
        "dynamic": "strict",
        "_routing": {"required": True},
        "properties": {
            "orgno": {
                "type": "long"
            },
            "company_name": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "contacts_designation_labels_2": {
                "type": "text",
                "term_vector": "yes",
                "fielddata": True,
            },
            "contacts_designation_labels_v2": {
                "type": "text",
                "term_vector": "yes",
                "fielddata": True,
            },
            "country_code": {"type": "keyword"},
            "created_time": {"type": "date"},
            "indexed_at":   {"type": "date"},
            "industry_labels": {
                "type": "text",
                "term_vector": "yes",
                "fielddata": True,
            },
        },
    },
}

# ── reference data pools ──────────────────────────────────────────────────────
COUNTRY_CODES = [
    "SE", "NO", "DK", "FI",   # Nordic core
    "DE", "NL", "GB", "FR", "PL", "ES", "IT",
]
COUNTRY_WEIGHTS = [25, 20, 15, 10, 8, 5, 5, 4, 3, 3, 2]

DESIGNATIONS = [
    "CEO", "CFO", "CTO", "COO", "CMO", "CHRO", "CPO",
    "VP Sales", "VP Marketing", "VP Engineering", "VP Finance",
    "Head of Sales", "Head of Marketing", "Head of HR", "Head of IT",
    "Sales Manager", "Account Manager", "Business Development Manager",
    "Product Manager", "Project Manager", "Operations Manager",
    "Director", "Managing Director", "Board Member", "Partner",
    "Senior Consultant", "Consultant", "Analyst", "Associate",
    "Software Engineer", "Data Engineer", "DevOps Engineer",
]

INDUSTRIES = [
    "Technology", "Software", "SaaS", "FinTech", "HealthTech", "EdTech",
    "Manufacturing", "Retail", "E-commerce", "Logistics", "Supply Chain",
    "Finance", "Banking", "Insurance", "Real Estate", "Construction",
    "Healthcare", "Pharmaceuticals", "Biotechnology", "Medical Devices",
    "Energy", "Renewable Energy", "Oil & Gas", "Utilities",
    "Consulting", "Professional Services", "Legal", "Accounting",
    "Media", "Advertising", "Marketing", "Public Relations",
    "Telecommunications", "IT Services", "Cybersecurity", "Cloud Computing",
    "Food & Beverage", "Agriculture", "Travel", "Hospitality",
    "Automotive", "Aerospace", "Defense", "Chemicals",
]


def _random_designations(n: int = None) -> str:
    count = n or random.randint(1, 5)
    return " ".join(random.sample(DESIGNATIONS, min(count, len(DESIGNATIONS))))


def _random_industries(n: int = None) -> str:
    count = n or random.randint(1, 4)
    return " ".join(random.sample(INDUSTRIES, min(count, len(INDUSTRIES))))


def _random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    delta = random.randint(end_days_ago, start_days_ago)
    dt = datetime.now(timezone.utc) - timedelta(days=delta)
    return dt.isoformat()


def _generate_doc(i: int, index: str) -> dict:
    # orgno: 6-9 digit org registration number (Nordic style)
    orgno = random.randint(100_000, 999_999_999)
    country = random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS, k=1)[0]
    created = _random_date(start_days_ago=1825)   # up to 5 years ago
    indexed = _random_date(start_days_ago=30)      # indexed recently

    return {
        "_index":   index,
        "_id":      str(orgno),          # orgno as document ID
        "_routing": str(orgno),          # REQUIRED — routing must be provided
        "_source": {
            "orgno":         orgno,
            "company_name":  fake.company(),
            "contacts_designation_labels_2":  _random_designations(),
            "contacts_designation_labels_v2": _random_designations(),
            "country_code":   country,
            "created_time":   created,
            "indexed_at":     indexed,
            "industry_labels": _random_industries(),
        },
    }


@click.command()
@click.option("--count",    default=5000, show_default=True)
@click.option("--recreate", is_flag=True, help="Drop and recreate index if it already exists")
def main(count: int, recreate: bool) -> None:
    index  = os.getenv("SOURCE_INDEX", "mmh-poc")
    client = source_client()

    wait_for_green(client, "source cluster")
    print_cluster_info(client, "SOURCE")

    if client.indices.exists(index=index):
        if recreate:
            client.indices.delete(index=index)
            console.print(f"[yellow]Dropped '{index}'[/yellow]")
        else:
            console.print(f"[yellow]'{index}' already exists — pass --recreate to reset[/yellow]")
            print_index_stats(client, index)
            return

    client.indices.create(index=index, body=INDEX_BODY)
    console.print(
        f"[green]✓ Created '{index}' "
        f"[{_SOURCE_SHARDS}p × {_SOURCE_REPLICAS}r, dynamic=strict, routing=required][/green]"
    )

    actions = (_generate_doc(i, index) for i in range(count))
    successes, errors = bulk(
        client,
        track(actions, description=f"Indexing into {index}...", total=count),
        chunk_size=500,
    )

    client.indices.refresh(index=index)
    console.print(f"[green]✓ {successes} docs indexed  ({errors} errors)[/green]")
    print_index_stats(client, index)

    target_shards = int(os.getenv("TARGET_SHARDS", "20"))
    target_replicas = int(os.getenv("TARGET_REPLICAS", "1"))
    console.print(
        f"\n[dim]Production analogy  : {_SOURCE_SHARDS}p×{_SOURCE_REPLICAS}r = "
        f"{_SOURCE_SHARDS*(_SOURCE_REPLICAS+1)} total shards, ~180 GiB[/dim]"
    )
    console.print(
        f"[dim]After reindex target: {target_shards}p×{target_replicas}r = "
        f"{target_shards*(target_replicas+1)} total shards (right-sized)[/dim]"
    )


if __name__ == "__main__":
    main()
