#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# Full POC run-through
#
# Mirrors production mmh-poc migration:
#   SOURCE  mmh-poc      5 primary × 1 replica = 10 shards  (~180 GiB in prod)
#   TARGET  mmh-poc-v2  20 primary × 1 replica = 40 shards  (right-sized)
#
# Local demo uses 5 000 docs.  Shard ratios and every API call are identical
# to what will run against the real clusters.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

step() {
  echo
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  printf "  %s\n" "$*"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

step "Step 0 – Start MinIO + both OpenSearch clusters"
docker compose up -d

echo "Waiting 60 s for clusters to stabilise (MinIO → keystore inject → OS green)…"
sleep 60

# step "Step 1 – Create mmh-poc on source  [5p×1r = 10 shards, 5 000 docs]"
# uv run python -m scripts.step01_setup_source --recreate

# step "Step 2 – Register S3 repo on source and take snapshot → MinIO"
# uv run python -m scripts.step02_create_snapshot

# step "Step 3 – Register S3 repo on target (readonly) and restore snapshot"
# uv run python -m scripts.step03_restore_snapshot

# step "Step 4 – _reindex mmh-poc-restored → mmh-poc-v2  [20p×1r = 40 shards]"
# uv run python -m scripts.step04_reindex_target

# step "Step 5 – Simulate live traffic: add 200 delta docs to source"
# uv run python -m scripts.step05_add_delta --count 200

# step "Step 6 – Remote _reindex delta from source → mmh-poc-v2"
# uv run python -m scripts.step06_remote_reindex

# echo
# echo "✅  POC complete."
# echo

echo "   SOURCE cluster"
echo "     OpenSearch API    :  http://localhost:9200"
echo "     Dashboards        :  http://localhost:5601   ← Dev Tools / Index Mgmt"
echo
echo "   TARGET cluster"
echo "     OpenSearch API    :  http://localhost:9201"
echo "     Dashboards        :  http://localhost:5602   ← Dev Tools / Index Mgmt"
echo
echo "   MinIO (S3) console  :  http://localhost:9001  (minioadmin / minioadmin)"
echo
echo "   Useful Dev Tools queries:"
echo "     GET _cluster/health"
echo "     GET _cat/indices?v&h=index,pri,rep,docs.count,store.size"
echo "     GET mmh-poc/_stats/store,docs"
