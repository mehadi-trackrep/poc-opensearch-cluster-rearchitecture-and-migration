"""
simulate_live_migration.py – live demo: stream docs into source while target catches up.

Two threads run concurrently:
  Producer  — indexes delta docs into SOURCE one-by-one with random delays
  Consumer  — remote-reindexes new docs SOURCE → TARGET every N seconds

A Rich live panel shows both counts updating in real time so you can watch
the target gradually close the gap.

Run:
    uv run python -m scripts.simulate_live_migration
    uv run python -m scripts.simulate_live_migration --count 200 --min-delay 0.2 --max-delay 1.0
"""

import os
import random
import threading
import time
from datetime import datetime, timezone

import click
from faker import Faker
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from scripts.common import console, source_client, target_client, wait_for_green
from scripts.step01_setup_source import (
    COUNTRY_CODES,
    COUNTRY_WEIGHTS,
    _random_designations,
    _random_industries,
)

fake = Faker()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _now_hms() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ── shared state ──────────────────────────────────────────────────────────────

class State:
    def __init__(self, src: int, tgt: int) -> None:
        self.lock          = threading.Lock()
        self.src_total     = src
        self.tgt_total     = tgt
        self.produced      = 0
        self.consumed      = 0
        self.last_sync_at  = "—"
        self.last_pulled   = 0
        self.syncs         = 0
        self.producer_done = False
        self.errors: list[str] = []


def _make_doc(indexed_at: str) -> tuple[int, dict]:
    orgno = random.randint(100_000, 999_999_999)
    return orgno, {
        "orgno":          orgno,
        "company_name":   fake.company(),
        "contacts_designation_labels_2":  _random_designations(),
        "contacts_designation_labels_v2": _random_designations(),
        "country_code":   random.choices(COUNTRY_CODES, weights=COUNTRY_WEIGHTS, k=1)[0],
        "created_time":   _now_iso(),
        "indexed_at":     indexed_at,
        "industry_labels": _random_industries(),
    }


# ── producer thread ───────────────────────────────────────────────────────────

def _producer(client, index: str, count: int, min_d: float, max_d: float, st: State) -> None:
    for _ in range(count):
        orgno, doc = _make_doc(_now_iso())
        try:
            client.index(index=index, id=str(orgno), routing=str(orgno), body=doc)
            with st.lock:
                st.produced += 1
        except Exception as exc:
            with st.lock:
                st.errors.append(f"[prod] {exc}")
        time.sleep(random.uniform(min_d, max_d))
    with st.lock:
        st.producer_done = True


# ── consumer thread ───────────────────────────────────────────────────────────

def _advance_since(src_client, src_index: str, since: str, batch: int, next_since: str) -> str:
    """After a partial pull (hit max_docs), find the indexed_at of the last pulled doc
    by querying source sorted asc and skipping (batch-1) entries.  That value becomes
    the new exclusive lower-bound so the next round continues from exactly that point."""
    resp = src_client.search(
        index=src_index,
        body={
            "query": {"range": {"indexed_at": {"gt": since}}},
            "sort":  [{"indexed_at": "asc"}],
            "size":  1,
            "from":  batch - 1,
            "_source": ["indexed_at"],
        },
    )
    hits = resp["hits"]["hits"]
    return hits[0]["_source"]["indexed_at"] if hits else next_since


def _consumer(
    tgt_client, src_client,
    src_index: str, tgt_index: str, src_host: str,
    since: str, poll: int, sync_batch: int, st: State,
) -> None:
    while True:
        time.sleep(poll)

        # Capture cutoff BEFORE the reindex so docs indexed during reindex
        # fall into the NEXT window, not lost between windows.
        next_since = _now_iso()

        try:
            src_client.indices.refresh(index=src_index)
            src_count = src_client.count(index=src_index)["count"]

            delta = src_client.count(
                index=src_index,
                body={"query": {"range": {"indexed_at": {"gt": since}}}},
            )["count"]

            pulled = 0
            if delta:
                source_body: dict = {
                    "remote": {"host": src_host},
                    "index":  src_index,
                    "size":   min(sync_batch, 500) if sync_batch else 500,
                    "query":  {"range": {"indexed_at": {"gt": since}}},
                }
                if sync_batch:
                    # Sort ascending so max_docs always takes the OLDEST docs first,
                    # giving us a deterministic cursor to advance since.
                    source_body["sort"] = {"indexed_at": "asc"}

                reindex_body: dict = {
                    "source": source_body,
                    "dest":   {"index": tgt_index, "op_type": "index"},
                    "script": {
                        "lang":   "painless",
                        "source": "ctx._source.remove('contacts_designation_labels_2');",
                    },
                }
                if sync_batch:
                    reindex_body["max_docs"] = sync_batch

                resp = tgt_client.reindex(
                    body=reindex_body,
                    params={"wait_for_completion": "true", "refresh": "true"},
                )
                pulled = resp.get("created", 0) + resp.get("updated", 0)

                if sync_batch and pulled >= sync_batch:
                    # Partial pull — advance since to the indexed_at of the last
                    # pulled doc so the next round continues from that exact point.
                    since = _advance_since(src_client, src_index, since, sync_batch, next_since)
                else:
                    since = next_since

            tgt_client.indices.refresh(index=tgt_index)
            tgt_count = tgt_client.count(index=tgt_index)["count"]

            with st.lock:
                st.src_total    = src_count
                st.tgt_total    = tgt_count
                st.consumed    += pulled
                st.last_sync_at = _now_hms()
                st.last_pulled  = pulled
                st.syncs       += 1

        except Exception as exc:
            with st.lock:
                st.errors.append(f"[cons] {str(exc)[:90]}")

        with st.lock:
            done  = st.producer_done
            equal = st.src_total == st.tgt_total

        if done and equal:
            break


# ── rich panel ────────────────────────────────────────────────────────────────

def _render(st: State, tgt_index: str, sync_batch: int = 0) -> Panel:
    with st.lock:
        src      = st.src_total
        tgt      = st.tgt_total
        produced = st.produced
        consumed = st.consumed
        syncs    = st.syncs
        last_at  = st.last_sync_at
        last_n   = st.last_pulled
        done     = st.producer_done
        errors   = list(st.errors[-3:])

    delta = src - tgt
    pct   = f"{100 * tgt // src}%" if src else "—"

    if done and delta == 0:
        status = "[bold green]✓ complete — source and target in sync[/bold green]"
    elif done:
        status = "[yellow]producer done — draining remaining delta…[/yellow]"
    else:
        status = "[cyan]streaming…[/cyan]"

    bar_width = 30
    filled = int(bar_width * tgt / src) if src else 0
    bar = "[green]" + "█" * filled + "[/green][dim]" + "░" * (bar_width - filled) + "[/dim]"

    t = Table.grid(padding=(0, 3))
    t.add_column(justify="right", style="dim", min_width=16)
    t.add_column()

    t.add_row("source docs",   f"[bold]{src:,}[/bold]")
    t.add_row("target docs",   f"[bold]{tgt:,}[/bold]  [dim]({pct})[/dim]")
    t.add_row("progress",      bar)
    t.add_row("pending delta", f"[yellow]{delta:,}[/yellow]" if delta else "[green]0[/green]")
    t.add_row("", "")
    t.add_row("produced",      f"{produced}")
    t.add_row("consumed",      f"{consumed}")
    t.add_row("sync rounds",   f"{syncs}")
    t.add_row("last sync",     f"{last_at}  [dim](+{last_n} docs)[/dim]")
    if sync_batch:
        t.add_row("batch size", f"{sync_batch} docs/round")
    t.add_row("", "")
    t.add_row("status",        status)

    if errors:
        t.add_row("[red]errors[/red]", "[red]" + " │ ".join(errors) + "[/red]")

    return Panel(
        t,
        title="[bold blue]Live Migration  SOURCE → TARGET[/bold blue]",
        subtitle=f"→ {tgt_index}",
        border_style="blue",
    )


# ── entry point ───────────────────────────────────────────────────────────────

@click.command()
@click.option("--count",         default=100,  show_default=True, help="Delta docs to stream into source")
@click.option("--min-delay",     default=0.3,  type=float, show_default=True, help="Min seconds between produced docs")
@click.option("--max-delay",     default=1.5,  type=float, show_default=True, help="Max seconds between produced docs")
@click.option("--poll-interval", default=8,    type=int,   show_default=True, help="Seconds between remote-reindex rounds")
@click.option("--sync-batch",    default=0,    type=int,   show_default=True, help="Max docs pulled per round (0 = unlimited)")
@click.option("--source-index",  default=lambda: os.getenv("SOURCE_INDEX", "mmh-poc"))
@click.option("--target-index",  default=lambda: os.getenv("TARGET_INDEX_REINDEXED", "mmh-poc-v2"))
@click.option("--source-host",   default="http://src-data-1:9200", show_default=True,
              help="Docker-internal address used by TARGET to reach SOURCE")
@click.option("--repo",     default=lambda: os.getenv("SNAPSHOT_REPO_NAME", "s3-repo"))
@click.option("--snapshot", default=lambda: os.getenv("SNAPSHOT_NAME", "mmh-poc-snapshot"))
@click.option("--since",    default=None, help="Override snapshot end_time as delta cutoff")
def main(
    count, min_delay, max_delay, poll_interval, sync_batch,
    source_index, target_index, source_host,
    repo, snapshot, since,
) -> None:
    src = source_client()
    tgt = target_client()
    wait_for_green(src, "source cluster")
    wait_for_green(tgt, "target cluster")

    if since is None:
        info  = src.snapshot.get(repository=repo, snapshot=snapshot)
        snap  = info["snapshots"][0]
        since = snap.get("end_time") or snap.get("start_time")

    src_count = src.count(index=source_index)["count"]
    tgt_count = tgt.count(index=target_index)["count"]

    console.print(f"[bold]Snapshot cutoff:[/bold] {since}")
    console.print(f"Source  '{source_index}'  :  {src_count:,} docs")
    console.print(f"Target  '{target_index}'  :  {tgt_count:,} docs")
    batch_str = f"  │  batch [bold]{sync_batch}[/bold] docs/round" if sync_batch else ""
    console.print(
        f"\nStreaming [bold]{count}[/bold] docs  "
        f"(delay {min_delay}–{max_delay}s per doc)  │  "
        f"sync every [bold]{poll_interval}s[/bold]{batch_str}\n"
    )

    st = State(src=src_count, tgt=tgt_count)

    producer = threading.Thread(
        target=_producer,
        args=(src, source_index, count, min_delay, max_delay, st),
        daemon=True,
    )
    consumer = threading.Thread(
        target=_consumer,
        args=(tgt, src, source_index, target_index, source_host, since, poll_interval, sync_batch, st),
        daemon=True,
    )

    producer.start()
    consumer.start()

    with Live(_render(st, target_index, sync_batch), refresh_per_second=4, console=console) as live:
        while consumer.is_alive():
            live.update(_render(st, target_index, sync_batch))
            time.sleep(0.25)
        live.update(_render(st, target_index, sync_batch))

    producer.join()
    consumer.join()

    console.print()
    console.print("[bold green]✓ Simulation complete[/bold green]")
    with st.lock:
        console.print(f"  Produced : {st.produced}")
        console.print(f"  Consumed : {st.consumed}")
        console.print(f"  Syncs    : {st.syncs}")
        if st.errors:
            console.print(f"  [red]Errors ({len(st.errors)}):[/red]")
            for e in st.errors:
                console.print(f"    [red]{e}[/red]")


if __name__ == "__main__":
    main()
