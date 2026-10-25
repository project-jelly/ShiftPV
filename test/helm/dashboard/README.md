# Dashboard query fixtures

| Cluster selector | Synthetic state |
|---|---|
| `preview-healthy` | Two fresh Pools with consistent Volume/Move holds |
| `preview-degraded` | One failed scan and invalid hold accounting, one `NeedsReview` Volume/Move |
| `preview-stale` | Last observation 10 minutes ago |
| `preview-never` | One node has never completed a successful observation |
| `preview-missing` | No metrics |

```bash
bash test/helm/dashboard/run.sh
```

The runner creates a temporary Prometheus bound to a random loopback port, validates all dashboard queries,
checks current-value semantics, and removes its container. It requires Docker, Go, curl and jq.
These recording rules are synthetic test data and stay outside the Helm package.
The alert fixtures also cover lifecycle failure/recovery, stale observations, transient CSI errors,
capacity waits, and six-hour pending continuity across exporter Pod replacement.

For visual checks, provision the product JSON in a temporary Grafana and use this Prometheus as its
datasource. Inspect Pool joins, Unknown cells, state text/color and Pool-only filtering in each scenario.
