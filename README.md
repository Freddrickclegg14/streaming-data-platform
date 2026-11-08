# Streaming Fraud Detection Platform

A real-time fraud detection pipeline built on the IBM Transactions Fraud dataset, using Kafka, PyFlink, and PostgreSQL. Second half of a two-project data engineering portfolio (the first being `batch-data-platform`), built to demonstrate stream-processing concepts the batch project couldn't: event-time windowing, keyed state, exactly-once checkpointing, and online fraud detection.

## Architecture

```
card_transaction.v1.csv
        │
        ▼
  replay_producer.py  ──────────────►  transactions.deadletter
        │ (validated JSON events,         (failed pydantic validation,
        │  is_fraud stripped out)          kept for inspection/replay)
        ▼
  transactions.raw (Kafka, 6 partitions, keyed by user_id)
        │
        ├──────────────────────┬──────────────────────┐
        ▼                      ▼                      │
  velocity_check_job     amount_anomaly_job            │
  (PyFlink, keyed         (PyFlink, keyed              │
   ListState)              ValueState)                 │
        │                      │                       │
        └──────────┬───────────┘                       │
                    ▼                                  │
              fraud_flags (Postgres)          ground_truth_labels (Postgres)
                    │                                   │
                    └───────────────┬───────────────────┘
                                     ▼
                      precision/recall evaluation (SQL join)
```

Both Flink jobs read the same Kafka topic independently (separate consumer group IDs) and write their flags to a shared table, distinguished by `rule_triggered`, so results can be compared side by side.

## Why the label never touches the live stream

`is_fraud` is a training label, not something a real-time system would have at transaction time — real fraud confirmation (chargebacks, investigations) arrives hours to weeks later, decoupled from the live stream. The producer splits it off before publishing: `transaction_id` (a deterministic hash of each row's position + content, so reruns are idempotent) ties the event and its label back together for evaluation, but the label itself goes straight to `ground_truth_labels` in Postgres, never through Kafka. `TransactionEvent` (the schema on the wire) excludes it entirely.

## Dead-letter handling

The pipeline is meant to simulate real-world messiness, not assume the clean synthetic CSV reflects reality. Any row that fails pydantic validation (malformed timestamp, bad amount) is routed to `transactions.deadletter` with its original raw content and failure reason — never silently dropped — so it can be inspected and replayed once the root cause is fixed. In practice, this dataset turned out to be too clean to meaningfully exercise this path (0 dead-lettered rows across 24.4M), which is itself worth noting: the mechanism is wired and tested at the unit level, but hasn't been proven against genuinely messy data. Deliberate chaos injection (late/duplicate/malformed events on the producer side) is a natural next step, not yet built.

## Reliability

Both Flink jobs checkpoint every 60s (`EXACTLY_ONCE`, `FileSystemCheckpointStorage`) and use a fixed-delay restart strategy (3 attempts, 10s apart). Without this, a transient Kafka connectivity blip — which happened more than once during development — kills the job outright with no recovery. The restart strategy is a known simplification: fixed-delay, not exponential backoff, which a production job would prefer to avoid hammering a struggling dependency during an outage.

## Governance: isolation level

Both Flink jobs write concurrently to `fraud_flags`. Postgres's default **READ COMMITTED** isolation is sufficient here, not SERIALIZABLE — each write is a single atomic `INSERT ... ON CONFLICT`, not a multi-step read-then-write sequence, so there's no window for a stale read to cause an anomaly. The actual concurrency bug found during development (two rules flagging the same transaction, with a single-column primary key on `transaction_id` silently dropping the second rule's flag via `ON CONFLICT DO NOTHING`) was a **schema** bug, not an isolation-level one — fixed by widening the primary key to `(transaction_id, rule_triggered)`. Worth documenting the distinction: reaching for a stricter isolation level wouldn't have fixed this.

## Testing

Rule logic (`check_velocity`, `check_amount_anomaly`) is extracted into plain functions in `flink_jobs/rules.py`, independent of Flink's runtime — the `KeyedProcessFunction` classes call them and manage state, but the decision logic itself is unit-testable with plain pytest, no live cluster needed. Same pattern as the batch project's `test_schemas.py`.

## Data modelling — and why there's no dbt here

Unlike the batch project (a real multi-layer warehouse: staging models feeding several mart tables with tests and a lineage graph), this pipeline's data model is flat — `ground_truth_labels` and `fraud_flags` are two OLTP-shaped tables, not a dimensional model, and bronze/silver here are nearly identical since validation already happens at the producer (same asymmetry documented for yfinance on the batch side). The one real aggregate ("gold layer" equivalent) is a single Postgres view:

```sql
CREATE OR REPLACE VIEW rule_performance AS
SELECT
    f.rule_triggered,
    COUNT(*) FILTER (WHERE g.is_fraud) AS true_positives,
    COUNT(*) FILTER (WHERE NOT g.is_fraud) AS false_positives,
    ROUND(COUNT(*) FILTER (WHERE g.is_fraud)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS precision_pct,
    ROUND(COUNT(*) FILTER (WHERE g.is_fraud)::numeric
        / NULLIF((SELECT COUNT(*) FROM ground_truth_labels WHERE is_fraud), 0) * 100, 2) AS recall_pct
FROM fraud_flags f
JOIN ground_truth_labels g USING (transaction_id)
WHERE f.flagged_fraud
GROUP BY f.rule_triggered;
```

`SELECT * FROM rule_performance;` reproduces the Results table below directly from live data — reproducible evidence, not a number to trust was copied correctly. Introducing dbt for one view would add orchestration machinery with nothing to orchestrate; it would earn its place if this grew into several related aggregate questions (e.g. performance by MCC, by hour of day), which is a natural extension, not built here.

## Orchestration & CI/CD

**CI** (`.github/workflows/ci.yml`) runs `ruff check` + `pytest` on every push — no cluster dependency, same pattern as batch's `ci.yml`.

**Orchestration** (`kestra/streaming_pipeline.yml`) sequences the operational steps: run the producer, then submit both Flink jobs. This is a deliberately thinner orchestration story than the batch project's Prefect flow, and that's an architectural difference worth stating plainly rather than glossing over — batch's Prefect flow had real retryable, dependent transform tasks; here, the ongoing work (stream processing) happens inside already-running Flink jobs, not inside the orchestrator, so Kestra is sequencing one-off steps rather than managing a DAG of transformations.

## Observability

The Flink UI (`:8081`) already gives per-job records-in/out, checkpoint history, and exceptions — proven useful during actual debugging, not just a nice-to-have. The `rule_performance` view above is the business-metric observability that actually matters (is detection working), queryable on demand. A Prometheus/Grafana stack scraping Flink's metrics reporter is the natural next step for a "real" setup, but is meaningfully more infrastructure to maintain for a portfolio project — documented here as a deliberate stopping point, not an oversight.

## Cloud deployment (Terraform)

`terraform/` provisions a single GCE VM (e2-standard-4, Debian 12) in its own GCP project (`dolapo-de-portfolio-streaming`), running the same Docker Compose stack as local — scoped down from a "real" cloud topology (managed Kafka, a multi-node Flink cluster, managed Postgres) to match this project's portfolio scope, same honest-tradeoff pattern as batch's Terraform. Verified with a real `terraform apply` → smoke test (producer run against a tiny sample, Postgres write confirmed) → `terraform destroy`, not just written and left unverified. Three genuine, non-obvious infrastructure bugs surfaced during that smoke test, worth recording as the actual value of testing on real infra rather than assuming code is correct:

1. **CRLF line endings in the Terraform-inlined startup script** caused the VM's boot script to fail silently (`exit status 127`, "required file not found" — the classic signature of a `\r` breaking a shebang line). Fixed by moving the script to a separate, LF-encoded `startup.sh` loaded via `file()`.
2. **Debian 12's default apt repos don't carry `docker-compose-plugin` under that name.** Switched to Docker's official `get.docker.com` install script instead of fighting repo configuration for a disposable VM.
3. **The project depended on plain `psycopg2`, which needs `pg_config` and a C compiler to build from source.** This worked by accident on Windows (a prebuilt wheel exists there) but failed hard on a bare Linux VM with no build toolchain. Fixed by switching to `psycopg2-binary` everywhere — a genuine cross-platform reproducibility fix, not just a cloud-specific workaround.

```bash
cd terraform
terraform apply -var="my_ip=$(curl -s ifconfig.me)/32"
# ... smoke test ...
terraform destroy -var="my_ip=$(curl -s ifconfig.me)/32"
```

## Results

Evaluated against the full, duplicate-free 24,386,900-row dataset (29,757 true fraud cases, ~0.12% base rate):

| Rule | True Positives | False Positives | Precision | Recall |
|---|---|---|---|---|
| `velocity_check` (≥5 txns/10min, same user) | 1,071 | 554,520 | ~0.19% | ~3.60% |
| `amount_anomaly_online` (online + >2× user's running avg) | 8,863 | 436,531 | ~1.99% | ~29.78% |
| **Combined (either rule)** | — | — | — | **~32.35%** |

Only 309 fraud cases were caught by both rules — limited redundancy, meaning they're largely complementary rather than detecting the same pattern twice.

**The velocity-check rule was a deliberate first attempt, and it failed in an instructive way.** It was built before looking at the data — a generic, "textbook" fraud heuristic. It performs barely above random guessing. Digging into the actual dataset (`analysis/fraud_characteristics.py`) showed fraud here is ~5.6× more likely to be an online transaction (61.7% vs 11.1%) and runs ~2.5× higher in amount on average ($108.59 vs $43.55) than legitimate transactions — neither of which velocity captures at all. The second rule, built from that finding, outperforms the first by roughly 10× on precision and 8× on recall. The lesson: a plausible-sounding fraud rule is not a substitute for looking at the data first.

## Known limitations

- **`amount_anomaly_online` can't flag a user's first transaction(s)** — there's no running average yet to compare against. Early fraud on a brand-new user is systematically undetectable by this rule, by design.
- **State has no TTL.** A user who stops transacting keeps their state in memory indefinitely. Fine at this scale; a production job would expire idle state.
- **Checkpointing uses fixed-delay restart, not exponential backoff.**
- **Single-broker Kafka, local-only** — no replication, no multi-broker failover. Changing broker listener configuration requires a full data-volume reset in this setup, which isn't representative of how a production cluster would handle it.
- **Sink is a direct JDBC-style write via `psycopg2`, not Flink's native JDBC connector** — simpler, avoids a second Java connector dependency, but less idiomatic Flink.
- **Dead-letter path is implemented and unit-tested but never exercised at scale** — the source data was too clean to trigger it.
- **Cloud deployment is a single VM running everything**, not a production-shaped topology (separate managed Kafka, real Flink worker nodes, managed Postgres) — a deliberate scope decision for a portfolio project, documented rather than implied to be more than it is.
- **No metrics dashboard (Prometheus/Grafana)** — the Flink UI and the `rule_performance` view cover what's needed to verify this pipeline works; a dedicated metrics stack is a reasonable next step, not built.

## Local setup

```bash
docker compose up -d
./scripts/create_topics.sh
uv run python -m producer.replay_producer --csv-path data/card_transaction.v1.csv
```

Submit the Flink jobs (containerized — PyFlink on Windows hit a persistent, unresolved subprocess failure running locally, so both jobs run inside the Flink cluster's own containers):

```bash
MSYS_NO_PATHCONV=1 docker exec jobmanager /opt/flink/bin/flink run -py /opt/flink/usrlib/velocity_check_job.py
MSYS_NO_PATHCONV=1 docker exec jobmanager /opt/flink/bin/flink run -py /opt/flink/usrlib/amount_anomaly_job.py
```

Flink UI: `localhost:8081` · Kafka UI: `localhost:8080`