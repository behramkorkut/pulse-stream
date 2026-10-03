# pulse-stream

[![CI](https://github.com/behramkorkut/pulse-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/behramkorkut/pulse-stream/actions/workflows/ci.yml)

A **real-time web analytics pipeline** written in Go: an HTTP collector feeds Kafka (Redpanda), a processor
enriches and sessionizes events, an aggregator keeps per-minute counters in MongoDB. It runs with Docker Compose
or on Kubernetes (kind + Helm), is observable with Prometheus and Grafana, and is load-tested and failure-tested
**with an end-to-end check that every accepted event is counted exactly once**.

> The events are **synthetic** and every number below was measured on one laptop. The goal is to demonstrate
> mechanisms (back-pressure, consumer groups, sessions, idempotence, observability, behaviour under failure), not
> to claim production scale. The deeper documents in `docs/` are written in French.

## What this project shows

- **A pipeline that absorbs bursts.** The collector sustains 16,000 requests/s with no errors (p99: 20 ms). What
  it cannot process immediately piles up in Kafka (up to 291,000 messages behind), then drains, with no loss.
- **Scaling by consumer groups.** 1 → 2 → 3 processors: 5,200 → 8,200 → 9,500 events/s end to end. The gain flattens
  when the single aggregator becomes the next bottleneck.
- **Correctness you can verify.** The load generator compares MongoDB to what the collector accepted, after every
  run, and exits non-zero on any difference.
- **Failure testing with honest results.** 9 recorded runs killing processors and aggregators under load (SIGKILL
  included): **no event lost** in any run; a rare **over-count** (up to 0.013 %) in 2 runs, traced to the
  non-atomic duplicate check between instances during a rebalance. Everything, including my own wrong assumptions,
  is in [docs/resilience.md](docs/resilience.md) and [docs/postmortem.md](docs/postmortem.md).
- **A production-style delivery chain.** Static distroless non-root images (19.6–35.4 MB), a Helm chart with
  probes, resource limits and a strict `securityContext`, Prometheus pod discovery, and a CI job that deploys
  the whole thing on a throwaway kind cluster and checks the counters.

## Architecture

```
load generator ──HTTP──▶ collector ──▶ Kafka/Redpanda [raw-events]
                                              │
                           processor (consumer group, N replicas)
              validate → enrich → drop bots → sessions (Redis, atomic Lua)
                                              │
                         Kafka [enriched-events] + [dead-letter]
                                              │
                    aggregator (consumer group) ──▶ MongoDB (counters per minute)
                         │ dedupe by site + event id (Redis)
                         ▼
        /metrics (every program) ──▶ Prometheus ──▶ Grafana
```

Key decisions, with their reasons, are in [docs/architecture.md](docs/architecture.md). The most important:

- **At-least-once delivery plus deduplication.** Offsets are committed only after processing; the aggregator
  applies counters, then remembers the event ids, then commits. A crash replays a batch, never loses one.
- **Partition key = site + visitor**, so one visitor's events stay ordered, which sessionization needs.
- **Open-loop load generation** (fixed rate, latency measured from the intended send time), so a slow server cannot
  hide its own saturation (coordinated omission).
- **Consumer lag summed over all partitions.** The library's built-in lag figure only sees one partition and
  under-reported by about 6×.
- **Event time with an explicit allowed lateness (1 h).** Late events land in the right minute; events more than an
  hour late (measured from reception, not processing) go to the dead-letter topic, because the duplicate memory and
  the session state are sized to that window. The load generator sends late and too-late events on purpose.
- **End-to-end freshness is measured**: a histogram of the time from collector reception to counters written in
  MongoDB (`pulse_end_to_end_latency_seconds`), shown as p50/p99 on the dashboard.
- **MongoDB for pre-aggregated counters**, because one atomic upsert with `$inc` per (site, minute) is exactly
  the write pattern. The trade-offs against PostgreSQL/TimescaleDB, ClickHouse and Druid/Pinot are discussed in
  [docs/architecture.md](docs/architecture.md).
- **Client IPs are truncated at the edge** (IPv4 /24, IPv6 /48) before reaching Kafka: the full address is never
  stored (GDPR data minimisation).

## Quick start

Requirements: Go, Docker (Colima or Docker Desktop) with `docker compose`, `make`, `git`.
For Kubernetes also `kind`, `helm` and `kubectl`. `make doctor` checks your tools.

Locally, programs on the host and infrastructure in Docker:

```bash
make up && make topics          # Redpanda, Redis, MongoDB, Prometheus, Grafana + topics
make run-processor              # one terminal each
make run-aggregator
make run-collector
make smoke                      # a few requests, with the expected status codes
make load RATES=200,1000,4000 DURATION=20s    # fixed-rate load + end-to-end check
make dashboard                  # Grafana, http://localhost:3000
```

Everything in containers (images built from one multi-stage `Dockerfile`):

```bash
make docker-build && make app-up && make smoke && make app-down
```

On Kubernetes (kind), three processors and three aggregators:

```bash
make kind-up docker-build kind-load
make helm-install HELM_ARGS="--set apps.processor.replicas=3 --set apps.aggregator.replicas=3"
make k8s-smoke
make k8s-load-verify RATES=3000 DURATION=30s     # load + exact check against MongoDB in the cluster
make k8s-chaos APP=aggregator MODE=crash KILLS=6 INTERVAL=20 RATES=2500 DURATION=150s
make k8s-dashboard                               # Grafana in the cluster, http://localhost:13000
```

`make help` lists every command.

## Ports

| Where | Collector | Prometheus | Grafana |
|---|---|---|---|
| Docker Compose / local | `localhost:8080` | `localhost:9090` | `localhost:3000` |
| kind | `localhost:18080` | `localhost:19090` | `localhost:13000` |

Metrics are on `:9101` (collector), `:9102` (processor) and `:9103` (aggregator) at `/metrics`. Logs are JSON; set
`LOG_LEVEL=debug` to see per-batch lines.

## Performance, briefly

Numbers come from a MacBook Pro (8 cores) running everything, load generator included. Protocol and limits:
[docs/benchmarks.md](docs/benchmarks.md). Absolute values do not transfer to a cluster; comparisons between
configurations do.

| Finding | Result |
|---|---|
| Collector throughput | 16,000 req/s, no errors, p99 20 ms |
| One processor alone | ~6,000 events/s; the surplus waits in Kafka (max 291,000 behind) and drains |
| 1 / 2 / 3 processors | 5,200 / 8,200 / 9,500 events/s end to end |
| End-to-end check | exact in every benchmark run |

## Failure behaviour, briefly

Details, method and mistakes: [docs/resilience.md](docs/resilience.md).

| Finding | Result |
|---|---|
| Events lost | none, in all 9 recorded runs |
| Over-counting | 2 runs out of 9, +49 events (0.013 %) and +2 events (0.0006 %) |
| Kafka session timeout 30 s → 10 s | consumer lag after a crash ~10× lower, cascading restarts gone |
| Known limit | the duplicate check is not atomic across instances; proposed fix: store the processed offset in the same MongoDB transaction as the counters |

## Repository layout

```
cmd/            collector, processor, aggregator, loadgen (one main each)
internal/       collector, processor, aggregator, batch (shared consume loop), sessions, dedupe,
                kafkautil (lag), metrics, loadgen, event, logging, version
deploy/         docker-compose helpers: prometheus, grafana (dashboard as JSON)
                helm/pulse-stream (the chart), kind (cluster config)
docs/           architecture, benchmarks, kubernetes, resilience, postmortem
scripts/        demo, smoke and chaos scripts used by the Makefile
Dockerfile      one multi-stage build for all programs (--build-arg CMD=...)
```

## Continuous integration

Every push and pull request runs (`.github/workflows/ci.yml`):

| Job | What it checks |
|---|---|
| Quality and unit tests | `go mod tidy` clean, `gofmt`, `go vet` (with and without integration tags), tests with the race detector and coverage, build |
| Integration tests | real Redpanda, Redis and MongoDB from the project's compose file |
| Known vulnerabilities | `govulncheck` |
| Docker images (×4) | builds each image, checks it does not run as root, smoke-tests the collector |
| Helm chart | `helm lint --strict` and rendering of three value combinations |
| Kubernetes deployment | creates a kind cluster, installs the chart with 2 processors, checks that Prometheus discovered 4 pods, then runs a load with the exact MongoDB check |

Everything in the first job runs locally with `make ci`. Dependabot proposes weekly updates for Go modules and
GitHub Actions. Integration tests share one infrastructure, so each test package uses its own Redis database
(12–15) because `go test` runs packages in parallel.

## Known limits

- One machine, one Kubernetes node, generator and cluster competing for the same cores.
- At-least-once with deduplication, **not** exactly-once: rare over-counting under rebalance (measured, see above).
- Infrastructure (Redpanda, MongoDB, Redis) runs as single, non-persistent instances in the chart.
- The processor and aggregator health probes only prove the process answers on `/metrics`; they do not check
  Kafka or MongoDB connectivity.
- The end-to-end check covers pageviews, clicks and bot events; per-session counters are not verified.
- A late event joins the visitor's current session even if it belonged to an earlier one (no session merging).

## Progress

| Step | Content | State |
|---|---|---|
| 0 | Skeleton, tooling, local infrastructure | done |
| 1 | HTTP collector: reception, validation, graceful shutdown | done |
| 2 | Kafka producer, topics, partition key | done |
| 3 | Processor: consumer group, enrichment, dead letter | done |
| 4 | Visitor sessions in Redis (atomic Lua script) | done |
| 5 | Aggregator: dedupe by id, per-minute counters, MongoDB upserts | done |
| 6 | Observability: Prometheus metrics, Grafana dashboard versioned in Git | done |
| 7 | Fixed-rate load generator with end-to-end counter check | done |
| 8 | Benchmarks: measured limits, reliable lag metric, effect of processor count | done |
| 9 | GitHub Actions CI: quality, tests, integration tests, vulnerabilities | done |
| 10 | Multi-stage distroless non-root images, programs in containers | done |
| 11 | Kubernetes: kind cluster, Helm chart, Prometheus pod discovery, deployment test in CI | done |
| 12 | Failure testing: SIGKILL under load, measured limits and fixes ([docs/resilience.md](docs/resilience.md)) | done |
| 13 | Final README and post-mortem ([docs/postmortem.md](docs/postmortem.md)) | done |

## License

[MIT](LICENSE)
