# Ray Telemetry → Databricks Pipeline

| Field          | Value                            |
|----------------|----------------------------------|
| Author         | xgui@anyscale.com                |
| Reviewers      | TBD                              |
| Created        | 2026-05-10                       |
| Updated        | 2026-05-11                       |
| Status         | Draft — for review               |
| Target release | TBD                              |

## 1. Summary

Pipe Ray cluster metrics into a centralized Databricks Delta lake by **reusing
the Prometheus instance users already run**. Users add a `remote_write` block
to their existing `prometheus.yml` pointing at a multi-tenant ingest service we
operate. The ingest service is an off-the-shelf OpenTelemetry Collector with a
`prometheusremotewrite` receiver and an `awss3` exporter; the Databricks
workspace runs Auto Loader against that S3 prefix to land a long-format Delta
table for SQL.

**Zero code changes in Ray.** No new exporter, no bundled binary, no install-size
impact. The Ray-side deliverable is a documentation page and a per-tenant token.

Default off. The existing Prometheus / Grafana / usage-stats paths are
untouched.

## 2. Motivation

We want SQL over Ray cluster metrics across many user clusters. Today Ray
exposes two telemetry surfaces, neither of which fits:

1. **Prometheus pull endpoint** (operational) — `/metrics` on the dashboard at
   `:44227` and per-node via `ReporterAgent`. Designed for a co-located Prometheus
   server to scrape into a local TSDB. No path to a warehouse, no remote storage.
2. **Usage stats POST** (product analytics) — one row per cluster per hour to
   `https://usage-stats.ray.io/`. Fixed dataclass schema, closed `TagKey` enum,
   no time-series semantics.

The key observation: every operator who deploys Ray for serious workloads already
runs Prometheus to scrape `/metrics` — that's the
[documented Ray monitoring setup](https://docs.ray.io/en/latest/cluster/kubernetes/k8s-ecosystem/prometheus-grafana.html).
Prometheus has a battle-tested **`remote_write`** facility that streams scraped
samples to any HTTPS endpoint with batching, retry, WAL-backed durability, and
configurable backpressure. We can stand on it instead of reinventing it in Ray.

## 3. Goals / Non-goals

**Goals**

- Push Ray's full metric surface (Python user metrics, Serve/Data/Train, raylet/GCS/scheduler internals) to Databricks Delta for SQL.
- **Zero Ray code changes** for v1. Pipeline lives entirely outside the Ray fork.
- Coexist with the Prometheus path — Grafana keeps working unchanged.
- Default OFF. Activated only when an operator explicitly configures `remote_write` on their Prometheus.
- One-time user setup: a `remote_write` block + a per-tenant token. No AWS/Databricks credentials on user machines.
- Survive transient ingest outages without dropping data (Prometheus's WAL handles this).

**Non-goals**

- Replace the usage-stats system. The closed-enum, anonymized POST stays for product analytics.
- Real-time metrics (sub-minute latency). v1 freshness budget is hourly, matched to the chosen S3 + Auto Loader landing.
- Reach customers who don't run Prometheus. They're out of scope for v1; addressing them is §13's "POST-from-Ray" alternative.
- Traces / logs. Out of scope; the architecture intentionally does not generalize beyond metrics.
- Pull-from-Prometheus (Databricks querying user Prom HTTP API). Ruled out: user Prometheus instances are not reachable from our central Databricks.

## 4. Background — current Ray telemetry surface

### 4.1 Prometheus/Grafana path

```
C++ stats (raylet/GCS) ─┐
                        ├─► MetricsAgent (Python)
Python user metrics ────┘    ├─ OpenTelemetryMetricRecorder
                             │    └─ PrometheusMetricReader  ──► :44227/metrics
                             └─ OpenCensus → prometheus_exporter ──► --metrics-export-port
                                                                          │
                                                            External Prometheus
                                                                  scrape
                                                                          ▼
                                                                    Grafana
```

What the user's Prometheus is already scraping today (full list in
`python/ray/dashboard/modules/reporter/reporter_agent.py:133-407`):

- **Node-level**: `node_cpu_utilization`, `node_cpu_count`, `node_mem_used/available/total`, `node_gpus_available`, `node_gpus_utilization`, `node_gram_used/available`, `node_gpu_power_milliwatts`, `node_gpu_temperature_celsius`, disk I/O (`node_disk_io_*`), network (`node_network_*`).
- **Per-process / per-component**: `component_cpu_percentage`, `component_rss_mb`, `component_uss_mb`, `component_num_fds`, `component_gpu_percentage`, `component_gpu_memory_mb`, broken down by `Component` (`raylet`, `gcs_server`, individual workers).
- **C++ raylet/GCS/scheduler internals**: task lifecycle counters, scheduler queue depths, object store metrics — proxied through `MetricsAgent`.
- **User application metrics**: anything emitted via `ray.util.metrics.Counter/Gauge/Histogram` — including Serve's `serve_replica_qps`, Train's training-loop metrics, custom application counters.

Because we're tapping at the Prometheus layer, **everything in that list flows
to Databricks for free.** Nothing has to be added to Ray.

Key files (for reviewer context):
- `python/ray/_private/telemetry/open_telemetry_metric_recorder.py` — Python OTel SDK MeterProvider with `PrometheusMetricReader`.
- `python/ray/_private/metrics_agent.py` — `OpencensusProxyMetric`, `ProxyMetricsCollector` (C++ → Python bridge).
- `python/ray/_private/prometheus_exporter.py` — legacy OC→Prom exporter for ReporterAgent.
- `python/ray/_private/telemetry/metric_cardinality.py` — `MetricCardinality.RECOMMENDED` already strips high-cardinality labels (e.g. `worker_id`) before export. **Whatever Prometheus sees today is what Databricks gets — same cardinality policy.**
- `python/ray/dashboard/modules/metrics/metrics_head.py` — Grafana provisioning, `RAY_GRAFANA_*` / `RAY_PROMETHEUS_*` env vars.
- `src/ray/stats/` — C++ side; OTel SDK pipeline gated by `enable_open_telemetry`.

### 4.2 Usage stats path

A separate, narrow "phone home" system: hourly POST of a closed `UsageStatsToReport`
dataclass to `https://usage-stats.ray.io/`. Documented here for contrast — it is
**not** the right shape for time-series metrics (wrong cadence, closed schema,
hard-coded destination, anonymized). The Databricks pipeline is a parallel
channel, not a replacement.

Key files: `python/ray/_common/usage/usage_lib.py`,
`python/ray/dashboard/modules/usage_stats/usage_stats_head.py`,
`src/ray/protobuf/usage.proto`.

## 5. Proposed architecture

```
User's Ray cluster (anywhere)                Centralized infra (us)
┌──────────────────────────────────────┐    ┌────────────────────────────┐
│ Ray (unchanged)                      │    │ Reverse proxy              │
│   - emits Prometheus /metrics        │    │   - terminates TLS         │
│         on :44227 and per-node       │    │   - validates per-tenant   │
│                                      │    │     bearer token           │
│ User's Prometheus (already running)  │    │   - sets X-Tenant-Id       │
│   - scrapes Ray as today             │    │     header from token map  │
│   - NEW: remote_write block ──┐      │    │             │              │
│     · filters to ray_.*       │      │    │             ▼              │
│     · WAL-buffered            │      │    │ Ingest Collector           │
│     · retry/backoff           │      │    │ (off-the-shelf otelcol)    │
│                               │      │    │   - prometheusremotewrite  │
│ Grafana (unchanged)           │      │    │     receiver               │
│   - reads from user's Prom    │      │    │   - resource processor:    │
└───────────────────────────────┼──────┘    │     stamp ray.tenant_id    │
                                │ snappy+pb │     from header            │
                                │ remote-   │   - awss3 exporter         │
                                │ write     │     (OTLP-JSON, hourly)    │
                                └──────────►│                            │
                                            └────────────┬───────────────┘
                                                         ▼
                                              s3://your-bucket/
                                                tenant_id={id}/year=Y/
                                                  month=M/day=D/hour=H/
                                                         │
                                              Databricks Auto Loader
                                                         ▼
                                              Delta long table
                                              (ts, metric_name,
                                               attrs MAP, value)
```

**Topology decisions** (carried over and confirmed):

- **Tenancy**: centralized — single Databricks workspace, multi-tenant by `tenant_id`.
- **Network**: user network has outbound HTTPS only. Prometheus initiates the connection to us; no inbound holes on the user side. No connectivity from us to user-side Prometheus (which is why pull-from-Prom was ruled out).
- **Transport**: Prometheus `remote_write` over HTTPS (snappy-compressed protobuf). Mature, widely deployed protocol. Prometheus handles batching, retry, WAL buffering on the client side.
- **Freshness**: hourly to Delta. S3 + Auto Loader, no streaming.
- **Schema**: long table, partition by `date(ts)` and `tenant_id`.
- **Ingest**: stock OTel Collector with `prometheusremotewrite` receiver + `awss3` exporter — no custom application code.
- **Auth**: per-tenant bearer tokens validated at a reverse proxy in front of the Collector; the proxy sets a trusted `X-Tenant-Id` header that downstream processors stamp into the OTel resource. Tokens are minted out-of-band by us and pasted into the user's Prom config.

## 6. Detailed design — Ray-side

There is no Ray-side code change in v1.

The Ray PR (if any) is documentation only: a page under `doc/source/cluster/`
explaining how operators configure `remote_write` against the central ingest
endpoint and obtain a per-tenant token. This is a single markdown file plus a
link from the existing Prometheus/Grafana docs.

This is the entire point of choosing `remote_write` over the OTel/push-from-Ray
alternatives in §13.

## 7. Detailed design — Ingest service

### 7.1 Reverse proxy (auth boundary)

A standard reverse proxy (Envoy / nginx / Cloudflare / API Gateway — pick
whichever is native to our infra) sits in front of the Collector.

Responsibilities:
- Terminate TLS.
- Validate the `Authorization: Bearer <token>` header against the token store.
- On success, set an internal `X-Tenant-Id` header from the token → tenant mapping. **Strip any client-supplied** `X-Tenant-Id` first so a tenant cannot spoof another tenant's identity.
- On failure, return 401. No retry-loop opportunities.
- Rate-limit per token to bound damage from a compromised token.

The token store starts as a static config map (KV store keyed by token hash).
A small admin API mints, lists, and revokes tokens.

### 7.2 Ingest Collector (the heavy lifting, all config)

A stock `otelcol-contrib` binary. The full config is approximately:

```yaml
receivers:
  prometheusremotewrite:
    endpoint: 0.0.0.0:9090
    # The reverse proxy already terminated TLS and validated the token.
    # Trust X-Tenant-Id only because we set it ourselves.

processors:
  memory_limiter:
    limit_mib: 4000
    spike_limit_mib: 800
    check_interval: 5s
  resource:
    attributes:
      - key: ray.tenant_id
        from_context: X-Tenant-Id   # populated by reverse proxy
        action: insert
  batch:
    timeout: 300s        # 5-minute batches; hourly SLO is easy
    send_batch_size: 50000

exporters:
  awss3:
    s3uploader:
      region: us-east-1
      s3_bucket: ray-metrics-bronze
      s3_prefix: "tenant_id=${X-Tenant-Id}/"
      s3_partition: hour
    marshaler: otlp_json

service:
  pipelines:
    metrics:
      receivers: [prometheusremotewrite]
      processors: [memory_limiter, resource, batch]
      exporters: [awss3]
```

This is the entire ingest service. **No custom code.** Operating the service is
"run an otelcol Deployment with HPA" — standard for any team that already runs
observability infra.

### 7.3 What lands in S3

OTLP-JSON, one file per batch:

```json
{
  "resourceMetrics": [{
    "resource": {
      "attributes": [{"key": "ray.tenant_id", "value": {"stringValue": "acme"}}]
    },
    "scopeMetrics": [{
      "metrics": [{
        "name": "ray_node_gpus_utilization",
        "gauge": {
          "dataPoints": [{
            "attributes": [
              {"key": "GpuDeviceName", "value": {"stringValue": "A100"}},
              {"key": "GpuIndex",      "value": {"stringValue": "0"}},
              {"key": "cluster_id",    "value": {"stringValue": "abc123"}}
            ],
            "timeUnixNano": "1715425200000000000",
            "asDouble": 92.0
          }]
        }
      }]
    }]
  }]
}
```

Choosing **OTLP-JSON** (rather than raw Prometheus protobuf) for the S3 landing
format gives us:
- A self-describing format with proper type info (gauge vs counter vs histogram).
- Future-proofing: if we ever add other ingress paths (OTLP, JSON-push), the
  Databricks-side parser doesn't change.
- Well-supported tooling for sampling and inspection (`otelcol-contrib` itself
  can decode it).

## 8. Detailed design — Databricks side

Unchanged from the previous draft.

- **Auto Loader job** with `cloudFiles.format=json`, `cloudFiles.schemaLocation`
  configured, `Trigger.AvailableNow` on hourly schedule.
- **Bronze table** mirrors S3 layout: raw OTLP-JSON unchanged, partitioned by
  `tenant_id` and `date(ingest_ts)`.
- **Silver table** is what consumers query:

  ```
  CREATE TABLE ray_metrics_long (
      ts            TIMESTAMP,
      tenant_id     STRING,
      cluster_id    STRING,
      metric_name   STRING,
      attrs         MAP<STRING, STRING>,
      value         DOUBLE
  )
  PARTITIONED BY (date(ts), tenant_id);
  -- Optional liquid clustering on metric_name.
  ```

- **Transform** is a Databricks notebook / DLT pipeline that explodes
  `resourceMetrics[].scopeMetrics[].metrics[]`, projects `name`, attributes,
  timestamp, and the gauge / sum / histogram data-point value into long rows.
  Histograms are flattened to one row per bucket with `attrs.le =
  "<bucket_upper>"` (standard Prometheus convention) so existing PromQL-style
  queries translate cleanly.

## 9. Security & privacy

### 9.1 Credential locality

```
User side                           Our side
┌──────────────────────────┐       ┌──────────────────────────┐
│ Has:                     │       │ Has:                     │
│  - Ingest URL            │ HTTPS │  - S3 IAM role           │
│  - Per-tenant bearer     │──────►│  - Databricks creds      │
│    token (in prom.yml)   │  +    │  - Token issuance signer │
│                          │ token │                          │
│ Cannot:                  │       │ S3 bucket policy:        │
│  - reach S3              │       │  - write: ingest role    │
│  - list other tenants    │       │  - read: pipeline +      │
│  - read Delta tables     │       │    analysts only         │
└──────────────────────────┘       └──────────────────────────┘
```

The user's Prometheus stores the bearer token in its config file (typically
`prometheus.yml` or a referenced `bearer_token_file`). Standard practice; same
shape as Prometheus's existing integrations (Grafana Cloud, AWS Managed
Prometheus, etc.).

Token leak blast radius: an attacker can write garbage Prometheus samples under
that `tenant_id` until we rotate. They cannot exfiltrate other tenants' data,
cannot reach S3, cannot reach Databricks. Rotation = mint new token, update
`bearer_token_file`, `kill -HUP` Prometheus.

### 9.2 Tenant isolation at ingest

The reverse proxy is the trust boundary. Two invariants worth highlighting:

- **Strip then stamp**: the proxy explicitly **deletes** any client-supplied
  `X-Tenant-Id` header before stamping its own from the token mapping. A
  malicious client cannot forge the resource attribute.
- **No cross-tenant writes**: the Collector's `awss3` exporter computes the
  prefix from the resource attribute, which is now trusted. A tenant's samples
  cannot land under another tenant's prefix.

### 9.3 Data review

The samples shipped via `remote_write` are exactly the metrics already exposed
on the user's Prometheus `/metrics` endpoint, post-`MetricCardinality.RECOMMENDED`
filtering. No new categories of data are emitted by Ray. Security review item:
(a) confirm no Prometheus-emitted attribute contains user-content or PII,
(b) document the `write_relabel_configs` filter (§10) so operators know exactly
which series leave their network.

### 9.4 Egress

`remote_write` is initiated from the user side over outbound HTTPS:443.
No inbound firewall holes. No new daemons running on user machines. Compatible
with standard `HTTPS_PROXY` / `NO_PROXY` settings via Prometheus's `proxy_url`
option.

### 9.5 On-disk buffer

Prometheus already maintains a WAL on disk for `remote_write` durability. If
the ingest endpoint is unreachable, samples queue locally and are replayed when
connectivity returns. This is **Prometheus's existing behavior**, not anything
new we're introducing. Operators tune retention via `--storage.tsdb.retention.time`
and `--storage.tsdb.wal-segment-size` flags they already understand.

## 10. The user-side change

A single block appended to the operator's existing `prometheus.yml`:

```yaml
remote_write:
  - url: https://ingest.<your-domain>/api/v1/write
    name: ray-databricks
    bearer_token_file: /etc/prometheus/ray-databricks-token

    # Only ship Ray metrics — exclude anything else the user's Prom scrapes.
    write_relabel_configs:
      - source_labels: [__name__]
        regex: 'ray_.*'
        action: keep

    # Optional resource attrs the operator wants attached at source.
    # The tenant_id is set server-side from the token; do not set it here.
    metadata_config:
      send: true
      send_interval: 1m

    queue_config:
      capacity: 50000
      max_samples_per_send: 5000
      max_shards: 30
      min_backoff: 30ms
      max_backoff: 5s
```

The operator obtains the `bearer_token_file` from a one-time mint API (or
support ticket — TBD in §11). They restart or `kill -HUP` Prometheus to pick
it up.

## 11. Rollout / phasing

| Phase | Scope | Risk |
|---|---|---|
| **1. Ingest stand-up** | Deploy reverse proxy + Collector + S3 bucket. Internal-only with a static test token. Validate against a dev Prometheus. | Low. All off-the-shelf infra. |
| **2. Databricks pipeline** | Auto Loader job, bronze → silver transform, Delta schema, sample queries. Can run in parallel with phase 1. | Low. Standard Auto Loader work. |
| **3. End-to-end with dogfood Ray cluster** | Configure a real Ray cluster's Prometheus to `remote_write` to ingest. Validate schema, cardinality, costs. | Low–medium. Mostly catches cardinality surprises in S3 storage cost. |
| **4. Token-issuance UX** | Decide between admin API, ticket-based mint, or self-service portal. Build minimum viable flow. | Medium. Cross-team product decision more than engineering. |
| **5. Onboarding docs + GA** | User-facing doc page, internal runbook, security review sign-off, on-call playbook. | Low. |

Each phase is independent and small. The total v1 effort is dominated by token
UX (phase 4), not by the technical pipeline.

## 12. Open questions

1. **Token-issuance UX.** Self-service portal vs. ticket-based mint vs. admin
   API only. Affects time-to-onboard and ops load. Not blocking the technical
   design, but blocks public rollout.
2. **Histogram representation in Delta.** One row per bucket (Prometheus
   convention, easy SQL) vs. a struct column with `bucket_boundaries` and
   `counts` arrays (compact, hard to query). v1 picks the former; flagged in
   case reviewers feel strongly.
3. **Metadata enrichment timing.** The `metadata_config` block sends `# TYPE`
   / `# HELP` metadata from Prometheus. Do we use it in the Databricks
   transform to label series properly, or rely on a Ray-shipped metric
   catalog? The former is self-contained; the latter is faster to query.
4. **Cardinality budget per tenant.** Should the ingest service enforce a
   max-series-per-tenant guard? Prometheus client already has
   `max_shards`/queue limits, but a bad-actor tenant could still flood. Lean
   toward "no for v1, alert on it" then tighten later.
5. **What to do for customers who don't run Prometheus.** Out of v1 scope by
   §3, but the question will come up. The §13 "POST-from-Ray" alternative is
   the natural fallback.
6. **Retention policy in S3 and Delta.** 90 days hot in Delta, archive to cold
   tier after? Needs a separate cost analysis.

## 13. Risks

- **Dependence on user Prometheus being correctly configured.** If a user
  changes scrape intervals, drops a scrape job, or breaks their Prometheus,
  the data simply stops arriving. We need monitoring on our side ("haven't seen
  data from tenant X in N hours") and clear runbooks for support.
- **Cardinality blowup costs.** Even with `MetricCardinality.RECOMMENDED`, the
  cross-product of metric names × attribute sets is much larger than usage-stats.
  Mitigation: liquid clustering on `metric_name`, retention policy on the long
  table, alert on per-tenant series counts.
- **`write_relabel_configs` correctness.** The `keep ray_.*` filter is critical;
  if a user omits it, their entire scrape surface (including any non-Ray apps
  Prometheus is monitoring) gets shipped to us. Mitigation: documentation
  prominence, and a server-side relabel that drops non-`ray_*` series as a
  safety net.
- **Token leakage via Prometheus config.** `prometheus.yml` often ends up in
  Git or config-management systems. Strongly recommend `bearer_token_file`
  with a separate secrets path, never inline `bearer_token`. Document in the
  onboarding guide.
- **Ingest-side outage**. If ingest goes down, Prometheus's WAL buffers up to
  its retention. Beyond that, samples are lost. Mitigation: ingest HA + SLO
  on availability + clear documentation of the user-side buffer cap.
- **Locked into Prometheus protocol.** If Prometheus or its `remote_write`
  semantics change incompatibly (currently `0.1.0` is stable, `2.0` in
  development), we need to handle both. The OTel Collector receiver does this
  for us today; risk is low but worth noting.

## 14. Alternatives considered

- **Push from Ray via OTLP + bundled OTel Collector.** Previous draft of this
  document. Future-proof, supports traces/logs, scales to sub-minute cadence,
  but requires substantial Ray code changes (Python OTLP exporter, C++ OTLP
  exporter, OpenCensus consolidation), bundles a 30–100 MB otelcol binary into
  Ray wheels, and adds 50–200 MB runtime RSS when enabled. Rejected for v1
  on the basis that `remote_write` reaches the same outcome with zero Ray
  changes. Reconsider if/when we need traces, logs, or sub-minute freshness.
- **Push from Ray via simple JSON POST ("like usage stats but for metrics").**
  Modeled on `UsageStatsHead`. ~300 LOC in one new dashboard module. Easier
  than the OTLP path but still requires a Ray PR, bypasses Prometheus's WAL,
  and forces us to reimplement batching/retry/backpressure that
  `remote_write` does for free. Kept as the fallback for customers who don't
  run Prometheus.
- **Pull from Prometheus (Databricks notebook queries user Prom HTTP API).**
  Architecturally simplest. Rejected: user Prometheus instances are not
  reachable from our central Databricks (firewall / VPC isolation). Would
  require VPN or a sidecar — both negate the simplicity.
- **Direct write from agent to S3.** Would require shipping AWS credentials
  to user machines. Rejected on §9 grounds regardless of which Ray-side
  path we pick.
- **Reuse the usage-stats endpoint.** Wrong shape: closed enum, hourly batch,
  anonymized analytics endpoint, no time-series semantics.
- **Wide Delta table per metric.** Faster scans for known metrics but every
  new Ray metric requires a schema migration. The long-table shape evolves
  for free.
- **Kafka streaming path for sub-minute freshness.** Out of scope for v1's
  hourly SLO. The architecture supports it as a future swap of the
  Collector's exporter or as a parallel pipeline.

## 15. Testing plan

- **Receiver protocol conformance.** Stand up the ingest service against the
  Prometheus `remote_write` compliance test suite. Validate snappy decoding,
  metadata propagation, and timestamp handling.
- **End-to-end smoke.** A CI job spins a Ray cluster + scraping Prometheus +
  the ingest stack in containers, asserts a known metric appears in a test
  Delta-equivalent (parquet on local disk) within one hour-batch.
- **Auth.** Unit test the reverse-proxy auth extension: valid token →
  `X-Tenant-Id` stamped, invalid → 401, missing → 401, client-supplied
  `X-Tenant-Id` stripped before stamping.
- **Tenant isolation.** Multi-tenant integration test: two tokens, two
  different `tenant_id`s, verify samples land under correct S3 prefixes and
  never cross.
- **Failure modes.** Ingest down → Prometheus WAL buffers, samples replay
  on recovery. Token revoked → 401 with clear error in Prometheus logs.
- **Cardinality regression.** Track per-tenant series count over time;
  alert on >2× week-over-week growth.

## 16. Appendix — file inventory

This pipeline is largely infra and Databricks work, not Ray code.

| Path | Change |
|---|---|
| `doc/source/cluster/metrics/remote-write-to-databricks.md` | **NEW** — operator onboarding doc (the only Ray-repo artifact). |
| (infra repo) `ingest/otelcol-config.yaml` | New stock-otelcol config. |
| (infra repo) `ingest/proxy/` | Reverse-proxy auth config. |
| (infra repo) `ingest/tokens/` | Token store + mint admin API. |
| (infra repo) `terraform/s3.tf` | S3 bucket + IAM. |
| (Databricks workspace) Auto Loader job | New ingestion job (S3 → bronze). |
| (Databricks workspace) Silver transform notebook | New transform (bronze → `ray_metrics_long`). |
| (Databricks workspace) `ray_metrics_long` Delta table | New table + DDL. |

No files in `python/ray/` or `src/ray/` are modified for v1.
