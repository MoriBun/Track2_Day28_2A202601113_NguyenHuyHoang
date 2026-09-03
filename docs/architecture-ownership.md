# Architecture & Ownership — Day 28 Track 2

- **Sinh viên:** Nguyễn Huy Hoàng — 2A202601113 (làm cá nhân)
- **Nhánh:** `ca-nhan-hoang`
- Sinh tự động từ `contracts/integration-matrix.yaml` — không gõ tay, nên không lệch với contract.

## Sơ đồ kiến trúc

![Kiến trúc Lab 28](images/lab28-architecture-overview.svg)

Sơ đồ gốc của lab (`docs/images/lab28-architecture-overview.svg`) thể hiện 5 layer và 10 boundary.
Bảng dưới bổ sung phần sơ đồ không có: **chủ sở hữu từng boundary**, contract hai đầu, health signal và trạng thái evidence thực tế.

## Vai trò

| Role | Phạm vi |
|---|---|
| `team-ingestion` | Ingestion & Orchestration — Kafka topics, producers, Airflow DAG, DLQ/replay |
| `team-data` | Data & ML — Spark/Delta, Feast repository, materialization, model release |
| `team-serving` | Serving & Retrieval — FastAPI orchestration, Qdrant, vLLM client, degraded paths |
| `team-platform` | Platform & Observability — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps |
| `team-presenter` | Presenter / Incident Commander — demo script, evidence pack, failure narration |

Làm cá nhân nên một người giữ cả năm vai trò; cột Owner dưới đây là vai trò *chức năng* cho từng boundary.

## Mười boundary — owner, contract, evidence

### IP01 — Data ingestion → Kafka

- **Layer:** L2 Data
- **Owner:** `team-ingestion` — Ingestion & Orchestration — Kafka topics, producers, Airflow DAG, DLQ/replay
- **Input:** contracts.FeedbackSubmission | contracts.DocumentSubmission (HTTP, via gateway)
- **Output:** contracts.IngestionEvent on topic data.raw, key = idempotency_key
- **Health signal:** Kafka broker reachable; data.raw topic exists with configured retention
- **Metrics:** `lab28_ingestion_events_total`, `lab28_ingestion_publish_seconds`
- **Tests:** IT-J1-golden-path, IT-J2-idempotent-replay, UT-contracts-ingestion
- **Readiness check:** `reliability.kafka_topics`
- **Evidence:** evidence/ip01-kafka-consume.json — message with traceparent header on data.raw
- **Trạng thái:** đạt

### IP02 — Kafka → Airflow pipeline

- **Layer:** L2 Data
- **Owner:** `team-ingestion` — Ingestion & Orchestration — Kafka topics, producers, Airflow DAG, DLQ/replay
- **Input:** contracts.IngestionEvent consumed from data.raw with W3C traceparent header
- **Output:** Airflow 3 asset event on asset lab28://delta/feedback; run_id recorded
- **Health signal:** Airflow api-server /api/v2/monitor/health reports scheduler+triggerer healthy
- **Metrics:** `lab28_pipeline_batches_total`, `lab28_consumer_lag`
- **Tests:** IT-J1-golden-path, IT-J4-degraded-recovery
- **Readiness check:** `reliability.airflow_pipeline`
- **Evidence:** evidence/ip02-airflow-run.json — DAG run id, task states, asset event
- **Trạng thái:** đạt (chứng minh bằng integration test)

### IP03 — Pipeline → Delta Lake / Lakehouse

- **Layer:** L2 Data
- **Owner:** `team-data` — Data & ML — Spark/Delta, Feast repository, materialization, model release
- **Input:** Batch of contracts.IngestionEvent (deduplicated by idempotency_key)
- **Output:** Delta MERGE into feedback/documents tables; new table version; _delta_log entry
- **Health signal:** DeltaTable.history() readable and version monotonically increasing
- **Metrics:** `lab28_delta_version`, `lab28_delta_rows_written_total`
- **Tests:** IT-J1-golden-path, IT-J2-idempotent-replay, UT-delta-merge-idempotency
- **Readiness check:** `reliability.delta_transaction_log`
- **Evidence:** evidence/ip03-delta-history.json — transaction log, schema, time travel diff
- **Trạng thái:** đạt

### IP04 — Lakehouse → Feature Store (Feast)

- **Layer:** L3 ML
- **Owner:** `team-data` — Data & ML — Spark/Delta, Feast repository, materialization, model release
- **Input:** Delta-derived offline snapshot at delta_root/exports/asker_activity
- **Output:** Feast online store rows for entity asker_id; feature service asker_serving_v1
- **Health signal:** Feast feature server /health 200 and get-online-features returns the entity
- **Metrics:** `lab28_feature_lookup_seconds`, `lab28_feature_freshness_seconds`
- **Tests:** IT-J1-golden-path, IT-J4-degraded-recovery
- **Readiness check:** `reliability.feature_store`
- **Evidence:** evidence/ip04-feast-online.json — entity row with delta_version and freshness
- **Trạng thái:** đạt

### IP05 — Data → Vector Store (embeddings)

- **Layer:** L2 Data
- **Owner:** `team-serving` — Serving & Retrieval — FastAPI orchestration, Qdrant, vLLM client, degraded paths
- **Input:** Document rows from Delta + pinned embedding model
- **Output:** Qdrant points in lab28_documents with deterministic UUID from doc_id
- **Health signal:** Qdrant /readyz 200 and collection point count > 0
- **Metrics:** `lab28_retrieval_seconds`, `lab28_vector_points`
- **Tests:** IT-J1-golden-path, IT-J2-idempotent-replay, UT-vector-stable-ids
- **Readiness check:** `reliability.vector_store`
- **Evidence:** evidence/ip05-qdrant-search.json — hybrid query result with scores and doc_ids
- **Trạng thái:** đạt

### IP06 — MLflow → Model Registry

- **Layer:** L3 ML
- **Owner:** `team-data` — Data & ML — Spark/Delta, Feast repository, materialization, model release
- **Input:** Evaluation run over the Delta version + retrieval config + vLLM model id
- **Output:** Registered model version with signature, tags, provenance and champion alias
- **Health signal:** MLflow /health 200 and get_model_version_by_alias(champion) resolves
- **Metrics:** `lab28_release_version_info`
- **Tests:** IT-J1-golden-path, IT-J3-promotion-rollback, UT-release-provenance
- **Readiness check:** `operations.model_rollback`
- **Evidence:** evidence/ip06-mlflow-release.json — version, signature, git sha, delta version
- **Trạng thái:** đạt

### IP07 — Model → vLLM / SGLang serving

- **Layer:** L1 Compute
- **Owner:** `team-serving` — Serving & Retrieval — FastAPI orchestration, Qdrant, vLLM client, degraded paths
- **Input:** Grounded prompt built from champion release config + retrieved sources
- **Output:** OpenAI-compatible chat completion from a real vLLM server; model id echoed
- **Health signal:** vLLM /health 200, /version reports a vLLM build, /metrics exposes vllm: series
- **Metrics:** `lab28_llm_seconds`, `lab28_llm_tokens_total`
- **Tests:** IT-J1-golden-path, IT-J3-promotion-rollback, IT-J4-degraded-recovery
- **Readiness check:** `reliability.inference_endpoint`
- **Evidence:** evidence/ip07-vllm-identity.json — /version + /v1/models + vllm: metric names
- **Trạng thái:** **UNVERIFIED**
- **Gate:** `gpu` — Requires a real GPU-backed vLLM 0.28 endpoint. A mock, a CPU classifier or any server that cannot produce vLLM's own /version and vllm: metrics fails this gate by design.

### IP08 — Serving → API Gateway

- **Layer:** L1 Compute
- **Owner:** `team-platform` — Platform & Observability — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps
- **Input:** External HTTP request to the gateway listener
- **Output:** Routed request with x-request-id; rate limit, auth policy and health routing
- **Health signal:** Gateway admin /ready 200; /healthz route answers without hitting the app
- **Metrics:** `envoy_http_downstream_rq_total`, `envoy_http_local_rate_limit_rate_limited`
- **Tests:** IT-J1-golden-path, IT-J5-trace-metrics-continuity, IT-gateway-rate-limit
- **Readiness check:** `security.gateway_policy`
- **Evidence:** evidence/ip08-gateway.json — 200 + 429 responses with x-request-id
- **Trạng thái:** đạt (chứng minh bằng integration test)

### IP09 — All components → Prometheus / Grafana

- **Layer:** L4 Ops
- **Owner:** `team-platform` — Platform & Observability — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps
- **Input:** Prometheus scrape of every service that exposes /metrics
- **Output:** Provisioned Grafana dashboards and at least one actionable SLO alert
- **Health signal:** Prometheus /api/v1/targets shows all expected jobs up
- **Metrics:** `up`, `lab28_request_seconds`
- **Tests:** IT-J5-trace-metrics-continuity, IT-prometheus-targets
- **Readiness check:** `observability.metrics_and_alerts`
- **Evidence:** evidence/ip09-prometheus-targets.json + evidence/ip09-grafana-dashboards.json
- **Trạng thái:** đạt (chứng minh bằng integration test)

### IP10 — All components → LangSmith tracing

- **Layer:** L4 Ops
- **Owner:** `team-platform` — Platform & Observability — gateway, OTEL collector, Prometheus, Grafana, K8s/GitOps
- **Input:** OTLP spans from gateway, API, Kafka, Airflow, Spark, Feast, Qdrant, vLLM
- **Output:** One trace ID spanning every boundary, exported to a local backend and LangSmith
- **Health signal:** Collector pipeline healthy; trace queryable by ID in the local backend
- **Metrics:** `otelcol_exporter_sent_spans`, `otelcol_exporter_send_failed_spans`
- **Tests:** IT-J5-trace-metrics-continuity, IT-trace-span-coverage
- **Readiness check:** `observability.trace_continuity`
- **Evidence:** evidence/ip10-trace.json — trace ID with the required span names
- **Trạng thái:** đạt (chứng minh bằng integration test)
- **Gate:** `langsmith` — The local OTLP backend provides deterministic offline and CI evidence. The LangSmith leg additionally requires LANGSMITH_API_KEY in the environment and is reported UNVERIFIED when no credential is supplied.

## Năm critical journey

| ID | Nội dung | Module | Phủ các IP |
|---|---|---|---|
| IT-J1-golden-path | Golden path across all ten integration points | `integration-tests/test_j1_golden_path.py` | IP01, IP02, IP03, IP04, IP05, IP06, IP07, IP08, IP09, IP10 |
| IT-J2-idempotent-replay | Duplicate/replay idempotency across Kafka, Delta, Feast and Qdrant | `integration-tests/test_j2_idempotent_replay.py` | IP01, IP02, IP03, IP04, IP05 |
| IT-J3-promotion-rollback | New release, champion promotion, serving change and rollback | `integration-tests/test_j3_promotion_rollback.py` | IP06, IP07 |
| IT-J4-degraded-recovery | Dependency failure, retry/DLQ/degraded response and recovery without data loss | `integration-tests/test_j4_degraded_recovery.py` | IP02, IP04, IP05, IP07 |
| IT-J5-trace-metrics-continuity | Trace, metrics and readiness continuity end to end | `integration-tests/test_j5_trace_metrics_continuity.py` | IP08, IP09, IP10 |

## Required spans cho IP10

- [x] `lab28.gateway.request`
- [x] `lab28.api.ingest`
- [x] `lab28.kafka.produce`
- [x] `lab28.kafka.consume`
- [x] `lab28.airflow.dag`
- [x] `lab28.spark.delta_merge`
- [ ] `lab28.api.ask`
- [ ] `lab28.feast.get_online_features`
- [ ] `lab28.qdrant.query`
- [ ] `lab28.mlflow.resolve_release`
- [ ] `lab28.vllm.chat_completion`

Trace đã quan sát: `68f76c6f81d64e7c8f8b8ac9776e1623` — 6/11 span, 3 process (lab28-airflow, lab28-api, lab28-gateway).
Năm span thiếu đều nằm sau `lab28.vllm.chat_completion` trên nhánh `/api/v1/ask`, bị gate bởi IP07.
