

## to live migration simulate using remote _reindex
```
uv run python -m scripts.simulate_live_migration \
  --count 200 \
  --min-delay 0.5 --max-delay 1.5 \
  --poll-interval 5 \
  --sync-batch 10
```

```
uv run python -m scripts.simulate_live_migration \
  --count 200 \
  --min-delay 0.5 --max-delay 1.5 \
  --poll-interval 5 \
  --sync-batch 10 \
  --since 2026-05-07
```

  ### 