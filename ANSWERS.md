# ANSWERS — Day 28 Track 2: Platform Integration & Production Readiness

- **Sinh viên:** Nguyễn Huy Hoàng — 2A202601113 (làm cá nhân)
- **Nhánh:** `ca-nhan-hoang`
- **Ngày chạy evidence:** 2026-09-03
- **Máy chạy:** Intel Core i5-1240P (12 nhân / 16 luồng), 15.7 GiB RAM, Windows 11 build 26200, Docker Desktop VM 7.6 GiB, **không có GPU NVIDIA** (chỉ Intel Iris Xe tích hợp)

---

## 0. Tóm tắt kết quả

| Kiểm tra | Lệnh | Kết quả |
|---|---|---|
| Fast suite | `uv run pytest starter-tests tests -q` | **87 passed** |
| Lint | `uv run ruff check .` | All checks passed |
| Matrix | `uv run python scripts/verify_matrix.py` | 245 checks passed |
| Portability | `uv run python scripts/check_portability.py` | OK |
| Manifests | `uv run python scripts/validate_manifests.py` | passed |
| Live suite | `uv run pytest integration-tests -m "not gpu and not langsmith" -q` | **56 passed, 16 deselected** (235s) |

`integration-report.json`: **score 83**, 5/6 verified point đạt, 4 point không chứng minh được từ trong process (đã chứng minh bằng integration test), 1 point `not_ready` là **IP07**.

### Trạng thái 10 integration point

| IP | Trạng thái | Evidence |
|---|---|---|
| IP01 | ✅ đạt | `ip01-kafka-consume.json` — key `it-j1-e45cf375`, trace `726a2052…`, header `traceparent` + `idempotency-key` dạng bytes |
| IP02 | ✅ đạt | `ip02-airflow-run.json` — DAG run, 4 task success, 4 asset event |
| IP03 | ✅ đạt | `ip03-delta-history.json` — feedback v0→v15 (15 MERGE), documents v9, time travel 0 rows → 25 rows |
| IP04 | ✅ đạt | `ip04-feast-online.json` — entity PRESENT, `delta_version` 16, freshness 78.4s |
| IP05 | ✅ đạt | `ip05-qdrant-search.json` — 20 point, embedding model pin theo commit SHA, có score và doc_id |
| IP06 | ✅ đạt | `ip06-mlflow-release.json` — `lab28-rag-release` **v5** là champion (promoted from v2), run `e7867d66…`, provenance tag đủ `delta_version` / `embedding_model_id` / `vllm_model_id` |
| IP07 | ⚠️ **UNVERIFIED** | `ip07-vllm-identity.json` — `reachable: false`. Xem mục 2. |
| IP08 | ✅ đạt | `ip08-gateway.json` — 10 accepted / 20 rate-limited trên 30 request, cả 200 và 429 đều có `x-request-id` |
| IP09 | ✅ đạt | `ip09-prometheus-targets.json` + `ip09-grafana-dashboards.json` — 9 job up, dashboard `lab28-platform`, 2 alert rule |
| IP10 | ⚠️ **một phần** | `ip10-trace.json` — trace `68f76c6f81d64e7c8f8b8ac9776e1623`, 6/11 required span, 3 service. Xem mục 2. |

---

## 1. Trade-off kỹ thuật đã chọn

### 1.1 `traceparent` bị bỏ hẳn thay vì gửi chuỗi rỗng (IP01/IP10)

`event_headers` trả `idempotency-key` luôn luôn, còn `traceparent` chỉ thêm khi thực sự có trace đang chạy. Header rỗng vẫn là header hợp lệ ở tầng Kafka nhưng lại là W3C traceparent **không hợp lệ**; consumer cố parse nó sẽ hoặc crash, hoặc tệ hơn là tự sinh một trace mới và làm đứt liên kết trace mà không báo lỗi. Bỏ hẳn header cho consumer một tín hiệu rõ ràng "không có context" để nó tự bắt đầu root span.

Đánh đổi: consumer phải xử lý nhánh "thiếu header", đổi lại không bao giờ phải xử lý nhánh "header có nhưng rác".

### 1.2 Dedupe theo `(occurred_at, event_id)` chứ không chỉ `occurred_at` (IP03)

Kafka không đảm bảo thứ tự giao giữa các partition, và hai event cùng `idempotency_key` hoàn toàn có thể mang cùng `occurred_at` (cùng millisecond). Nếu chỉ so `occurred_at`, kết quả MERGE phụ thuộc thứ tự Kafka giao — replay cùng dữ liệu có thể cho ra row khác nhau. Thêm `event_id` làm tie-break biến phép chọn thành **toàn phần và tất định**: cùng tập event vào, cùng row ra, bất kể thứ tự.

Kết quả sắp theo `idempotency_key` tăng dần cũng vì lý do đó — để batch gửi vào Spark MERGE có thứ tự tái lập được, giúp so sánh Delta version giữa các lần chạy.

Đánh đổi: `event_id` không mang ý nghĩa nghiệp vụ nên "event mới nhất" khi trùng timestamp là một lựa chọn tùy ý — nhưng *tất định*, và tất định quan trọng hơn "đúng theo trực giác" ở đây.

Input là `Iterable` và chỉ được duyệt **một lần** — code không gọi `len()` hay lặp lại lần hai, để generator từ Kafka consumer dùng được trực tiếp mà không phải nạp cả batch vào RAM.

### 1.3 `FEATURE_REFS` import từ `contracts`, không chép lại (IP04)

Bốn feature string là contract giữa Feast registry và request phía serving. Chép lại chúng ở `integration_tasks.py` tạo ra hai nguồn sự thật: đổi feature view trong Feast mà quên sửa bản chép sẽ cho lỗi `NOT_FOUND` lúc runtime thay vì lỗi lúc import. Import từ `contracts` khiến sai lệch trở thành lỗi ngay tại chỗ định nghĩa.

`full_feature_names=False` để key trả về là tên feature trần (`avg_rating`) chứ không phải `asker_activity_v1__avg_rating`, khớp với cách `feature_store.py` đọc kết quả.

### 1.4 `degraded` vẫn trả 200, chỉ `not_ready` mới trả 503 (IP07/IP08)

Đây là trade-off quan trọng nhất về vận hành. Thứ tự ưu tiên trong `readiness_status`: mandatory fail → `not_ready`; chỉ optional fail → `degraded`; còn lại → `ready`.

`/ready` trả 503 nghĩa là gateway **rút pod khỏi rotation**. Nếu coi Feast là mandatory thì một Feast lạnh sẽ rút *toàn bộ* pod khỏi rotation cùng lúc — biến một sự cố cục bộ (câu trả lời mất phần cá nhân hóa) thành mất dịch vụ toàn phần. Feast vì thế được báo cáo nhưng không mandatory: câu trả lời **tệ hơn, không phải sai**.

Điều này đã được chứng minh live trong J4 và trong một drill có ghi timestamp riêng (`evidence/failure-recovery.json`): dừng Feast lúc 10:19:56Z → sau ~4s `/ready` vẫn **HTTP 200** với `status: degraded`, component `feast` lật `true → false`, và **không bao giờ** thành `not_ready`/503; khởi động lại lúc 10:20:02Z → `feast` xanh trở lại. Không mất dữ liệu: Qdrant 20 → 20 point, Delta feedback v15 → v15, consumer group `lab28-pipeline` trên `data.raw` lag 0 → 0. Chỉ dùng `stop`/`start`, không `down -v`.

Phân biệt tương ứng ở tầng probe Kubernetes: `livenessProbe` trỏ `/health` (cố ý không chạm dependency nào) còn `readinessProbe` trỏ `/ready`. Nếu liveness cũng chạm dependency thì một sự cố Kafka sẽ khiến kubelet **restart** toàn bộ pod đang khỏe mạnh — đúng lúc hệ thống cần chúng nhất.

### 1.5 Retry 429 trong `lab28 seed` (thay đổi duy nhất ngoài `integration_tasks.py`)

Gateway cố ý giới hạn burst ở 10 rps (`max_tokens: 10`, `tokens_per_fill: 10`, `fill_interval: 1s`). Client đóng gói sẵn trong `seed --via-gateway` post tuần tự và không pace, nên bị 429 và **đếm nhầm 429 thành `rejected`** — trong khi 429 là mã retryable có tài liệu, không phải bản ghi bị từ chối. Tôi thêm retry với backoff 1s, tối đa 4 lần.

Đây là sửa client cho đúng contract của chính hệ thống, không phải nới lỏng test: không có test nào bị sửa, và rate limit của gateway vẫn được IP08 kiểm chứng nguyên vẹn (30 request → 10 accepted, 20 rate-limited).

---

## 2. Những gì KHÔNG chứng minh được và tại sao

Hai hạng mục dưới đây được báo cáo trung thực là chưa xác minh. Không có mock, không có endpoint giả OpenAI-compatible, không có evidence bịa.

### 2.1 IP07 — vLLM thật: UNVERIFIED

`evidence/ip07-vllm-identity.json`:

```json
{"reachable": false, "version": null, "served_models": [], "vllm_metric_names": [],
 "vllm_metric_count": 0, "is_real_vllm": false, "detail": "unreachable: ConnectError"}
```

**Lý do:** máy chỉ có Intel Iris Xe tích hợp, `nvidia-smi` không tồn tại, nên `compose.gpu.yaml` (Lựa chọn A) không chạy được. Lựa chọn B cần endpoint GPU Kaggle/lớp cấp — chưa có URL và credential được phép dùng, và lab cấm commit URL tunnel/token.

**Hệ quả trung thực:** `/api/v1/ask` trả **HTTP 503** `category=dependency_unavailable`, `retryable=true`, kèm `trace_id` — đúng degraded policy đã thiết kế, thay vì bịa ra câu trả lời không có grounding. Gate `gpu` trong `conftest.py` skip 16 test một cách có chủ đích; chúng bị **deselect chứ không phải pass giả**.

**Cần gì để đạt:** một endpoint vLLM ≥0.28 chạy trên GPU thật, chứng minh bằng `/version` trả build vLLM, `/v1/models` liệt kê model ID, và `/metrics` có series tiền tố `vllm:`. Server giả OpenAI-compatible **cố ý** trượt gate này.

### 2.2 IP10 — trace phủ toàn bộ 11 span: một phần

Trace `68f76c6f81d64e7c8f8b8ac9776e1623` đi qua **3 process** (`lab28-gateway`, `lab28-api`, `lab28-airflow`) với 6/11 required span:

✅ `lab28.gateway.request`, `lab28.api.ingest`, `lab28.kafka.produce`, `lab28.kafka.consume`, `lab28.airflow.dag`, `lab28.spark.delta_merge`

❌ `lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`

**Phần khó nhất đã chứng minh được.** Nửa bất đồng bộ là phần thực sự khó: consumer, DAG và Spark MERGE chạy trong ba process chưa từng thấy HTTP request gốc, nên việc chúng cùng mang một trace ID chỉ có thể đến từ header Kafka và run config — đúng thứ `event_headers` chịu trách nhiệm.

Năm span thiếu đều thuộc *serving leg*. Chúng nằm sau `lab28.vllm.chat_completion` trong cùng một request `/api/v1/ask`, mà request đó trả 503 tại bước vLLM. Trong `test_j5` và `test_trace_span_coverage`, serving leg được đánh dấu `@pytest.mark.gpu`, nên bị gate cùng IP07. **Sửa được IP07 thì IP10 tự đầy đủ** — không cần thay đổi code nào khác.

### 2.3 Argo CD sync / self-heal live: UNVERIFIED

Đã xác minh được (xem `evidence/gitops-validation.json`):
- `validate_manifests.py` pass
- `kubectl kustomize deploy/kubernetes/base` render sạch 10 resource
- Vòng promotion → rollback ở tầng Git: đổi tag image `3.0.0` → `3.1.0`, review diff, manifest vẫn valid; `git checkout --` khôi phục, `git diff --stat` rỗng, manifest vẫn valid

Chưa xác minh: `argocd app sync`, drift injection và self-heal thật. `kubectl` v1.29.2 có nhưng `kubectl cluster-info` không tìm thấy server; kind/minikube/kustomize/helm/argocd CLI đều chưa cài. Bật Kubernetes của Docker Desktop cần gần trọn 7.6 GiB VM mà stack 14 container đang giữ ~6.3 GiB — dựng cluster đồng nghĩa phá stack đang chạy và **hủy chính state mà evidence pack này dựa vào**.

---

## 3. Production gaps và cách khắc phục nếu có thêm thời gian/ngân sách

### 3.1 Ưu tiên cao

**GPU inference thật.** Gap lớn nhất, chặn IP07 và nửa IP10. Khắc phục: một node GPU (A10G/L4 là đủ cho Qwen3-4B) chạy vLLM sau autoscaler theo queue depth. Ngân sách chi phối trực tiếp: GPU là hạng mục đắt nhất và cũng là thứ duy nhất không thể mô phỏng.

**`/ready` quá nặng — đây là bottleneck đo được.** `/ready` fan-out tới Kafka, Delta, Qdrant, MLflow và Feast **trên mỗi lần gọi**. Với `periodSeconds: 10` và HPA tối đa 8 replica, đó là 8 lần fan-out mỗi 10 giây chỉ để phục vụ probe. Số đo cho thấy hậu quả: bỏ qua gateway, p50 tăng từ 487ms (8 worker) lên 1211ms (16 worker) — gấp ~2.5 lần độ trễ cho 2 lần concurrency, với 0 lỗi. Kafka giữ **207% CPU** trong lúc chạy tải, là consumer CPU lớn nhất trên máy.

Khắc phục: cache kết quả probe với TTL ~2–3s và refresh nền, để N probe đồng thời dùng chung một vòng fan-out. Việc này gần như chắc chắn dời điểm bão hòa lên trên đáng kể mà không đổi ngữ nghĩa readiness.

**Không có SLO có ngân sách lỗi.** Hai alert hiện có (`Lab28ApiUnavailable`, `Lab28HighErrorRatio` >5% trong 2 phút) là alert triệu chứng, không phải SLO. Khắc phục: định nghĩa SLI/SLO tường minh (mục 4.1) và alert theo **tốc độ đốt error budget** đa cửa sổ (5m/1h nhanh, 6h/3d chậm) thay vì ngưỡng tĩnh — ngưỡng tĩnh vừa ồn lúc traffic thấp vừa điếc lúc traffic cao.

### 3.2 Ưu tiên trung bình

**DLQ có nhưng chưa có quy trình replay tự động.** Topic `data.raw.dlq` tồn tại và có consumer group. Thiếu: alert theo độ sâu DLQ, và runbook phân biệt lỗi *độc* (bản ghi hỏng, không bao giờ thành công) với lỗi *tạm thời* (dependency down). Replay mù một bản ghi độc là vòng lặp vô hạn.

**Kafka một broker, không replication.** `replication.factor=1` — mất broker là mất dữ liệu chưa tiêu thụ. Production cần tối thiểu 3 broker, `min.insync.replicas=2`, `acks=all`.

**Không có kiểm soát schema evolution.** `IngestionEvent` có `schema_version: "1"` nhưng không có Schema Registry ép tương thích. Producer đổi contract sẽ làm hỏng consumer lúc runtime. Khắc phục: Schema Registry với chế độ `BACKWARD`, kiểm tra trong CI.

**Embedding model pin theo commit SHA nhưng không có quy trình re-index.** Model được pin `paraphrase-multilingual-MiniLM-L12-v2@faf4aa42…`. Đổi model làm mọi vector cũ không so sánh được với vector mới. Khắc phục: coi model ID là một phần identity của collection, re-index blue/green sang collection mới rồi mới chuyển alias.

### 3.3 Đã đúng, giữ nguyên

Manifest đã có sẵn phần lớn phần khó: `runAsNonRoot`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, `seccompProfile: RuntimeDefault`, `automountServiceAccountToken: false`, namespace ép `pod-security.kubernetes.io/enforce=restricted`, NetworkPolicy, HPA (CPU 70%, 2→8, scaleDown stabilisation 300s), PDB `minAvailable: 1`, ba probe tách bạch đúng vai trò, resource requests/limits đầy đủ. Argo CD Application pin `targetRevision: refs/tags/v3.0.0` với `selfHeal: true` và `prune: true` — tag bất biến là thứ khiến rollback thành thao tác Git chứ không phải `kubectl`.

---

## 4. SLO, security, observability và cost

### 4.1 SLO đề xuất

Số dưới đây suy ra từ đo đạc trên **laptop**, không phải production capacity — nêu ra làm điểm khởi đầu để hiệu chỉnh trên hạ tầng thật.

| SLI | Cách đo | SLO đề xuất | Cơ sở |
|---|---|---|---|
| Availability ingestion | tỉ lệ `POST /api/v1/{feedback,documents}` không trả 5xx | 99.5% / 30 ngày | Đã có `lab28_requests_total{status}` |
| Latency ingestion | p95 của `lab28_ingestion_publish_seconds` | p95 < 300ms | Ingestion chỉ validate + publish |
| Latency ask | p95 end-to-end `/api/v1/ask` | **chưa đặt được** | Chặn bởi IP07 |
| Freshness feature | `lab28_feature_freshness_seconds` | p95 < 15 phút | Đo được 78.4s ở J1 |
| Độ trễ pipeline | Kafka consumer lag | lag < 1000, hồi < 10 phút | Đo được **lag = 0** trên `lab28-pipeline/data.raw` |

Ngân sách lỗi 99.5% ≈ 3h39m/tháng. Alert theo tốc độ đốt: cửa sổ nhanh 5m/1h ở 14.4x, cửa sổ chậm 6h/3d ở 1x.

### 4.2 Security

**Đã có:** pod security `restricted`, container không root, filesystem chỉ đọc, không tự mount service account token, NetworkPolicy, gateway là điểm vào duy nhất với rate limit và `x-request-id` cho mọi response. `.gitignore` chặn `.env`, `.lab28/`, `evidence/`, database, cache và model weight — commit không chứa secret.

**Còn thiếu:**
- **Không có xác thực ở gateway.** Bất kỳ ai chạm tới listener đều ingest được. Cần OIDC/JWT ở Envoy, và **authorization** theo tenant chứ không chỉ authentication.
- **Rate limit theo listener, không theo caller.** 10 rps là ngân sách chung: một client ồn ào làm nghẹt tất cả. Cần rate limit theo API key/tenant.
- **Không có quản lý secret.** Config qua ConfigMap. Production cần External Secrets Operator hoặc CSI driver, không dùng Secret thuần base64.
- **Chưa có mTLS nội bộ.** Traffic giữa các service là plaintext trong cluster.
- **Chưa có ký/quét image.** Nên có cosign + Trivy trong CI, chặn deploy nếu có CVE nghiêm trọng.
- **PII.** Text feedback lưu thô trong Delta và Qdrant, không phân loại, không có TTL. Cần phân loại dữ liệu và chính sách lưu trữ trước khi có người dùng thật.

### 4.3 Observability

**Đã có:** metrics (Prometheus, 9 job up, dashboard `lab28-platform` provision sẵn, 2 alert rule), traces (OTLP → collector → Jaeger, trace ID xuyên 3 process qua ranh giới Kafka), và readiness có cấu trúc gán từng probe cho từng owner (`team-ingestion`, `team-data`, `team-serving`, `team-platform`).

Điểm mạnh nhất: **`/ready` và Prometheus không được phép bất đồng**. `test_j5` khẳng định từng component trong `/ready` khớp gauge tương ứng trong Prometheus, nên dashboard không thể lệch so với sự thật hệ thống.

**Còn thiếu:**
- **Log không tập trung và không tương quan với trace.** Không có Loki/ELK; debug phải `docker compose logs` từng service. Cần log có cấu trúc, nhúng `trace_id`, để nhảy thẳng từ span sang log.
- **Chưa có exemplar** nối metric sang trace mẫu.
- **Chưa có continuous profiling** — runbook yêu cầu "no memory leaks" nhưng chưa có công cụ chứng minh qua thời gian.
- **Alert chưa định tuyến.** Có rule nhưng không có Alertmanager, không có on-call, không có chính sách im lặng.

### 4.4 Cost / GPU

Toàn bộ platform (trừ GPU) chạy trong ~6.3 GiB trên một laptop — chi phí CPU/RAM không phải yếu tố chi phối. **GPU chi phối tất cả.**

Quan sát về chi phí từ số đo:
- Hai container nặng nhất là Spark Connect (1.76 GiB) và MLflow (1.45 GiB) — **cả hai đều không nằm trên đường request**. Production nên tách chúng khỏi node phục vụ traffic để scale độc lập; scale đường ask không cần kéo theo Spark.
- Kafka là consumer CPU lớn nhất (207% lúc tải). Với deployment thật, tuning broker rẻ hơn nhiều so với thêm replica API.
- Với GPU: chạy liên tục một node inference là chi phí cố định lớn. Vì `/api/v1/ask` đã có sẵn degraded path trả 503 retryable, kiến trúc **chịu được** scale-to-zero với cold start — đánh đổi độ trễ đuôi lấy chi phí, và đó là quyết định kinh doanh, không phải kỹ thuật.
- Batch prefill và continuous batching của vLLM là đòn bẩy chi phí lớn nhất mỗi token; đặt `--max-model-len` sát nhu cầu thật thay vì để tối đa giúp tăng đáng kể số request đồng thời trên cùng một GPU.

---

## 4b. Reflection

### Điều khó nhất

**Giữ trace sống qua ranh giới Kafka.** Mọi thứ khác trong bài là gọi một API và đọc kết quả. Riêng chỗ này thì consumer, Airflow DAG và Spark MERGE chạy trong ba process **chưa từng nhìn thấy HTTP request gốc**. Không có ngữ cảnh nào tự chảy sang; nếu `event_headers` không ghi `traceparent` vào header Kafka thì mỗi process sẽ lặng lẽ mở một trace mới và không có gì báo lỗi cả — test vẫn xanh, dashboard vẫn đẹp, chỉ có trace là đứt làm bốn mảnh. Một lỗi im lặng thì khó hơn nhiều so với một lỗi ồn ào.

Chi tiết khiến tôi phải dừng lại suy nghĩ là quyết định **bỏ hẳn** header khi không có trace, thay vì gửi chuỗi rỗng. Chuỗi rỗng vẫn là header Kafka hợp lệ nhưng là W3C traceparent hỏng — nó đẩy cái sai xuống consumer dưới dạng dữ liệu rác thay vì một tín hiệu rõ ràng "không có context". Đây là lúc tôi hiểu ra bài học lớn nhất của lab: ranh giới không chỉ là nơi truyền dữ liệu, mà là nơi phải quyết định *cái gì được phép vắng mặt*.

**Á quân:** phân biệt `degraded` với `not_ready`. Trực giác ban đầu của tôi là dependency hỏng thì báo không sẵn sàng. Trực giác đó sai, và sai theo hướng nguy hiểm: coi Feast là mandatory sẽ khiến `/ready` trả 503 → gateway rút *toàn bộ* replica → một sự cố cục bộ thành mất dịch vụ toàn phần. Cùng logic đó áp cho probe Kubernetes: `livenessProbe` cố ý không chạm dependency nào, vì nếu chạm thì sự cố Kafka sẽ khiến kubelet **restart** những pod đang hoàn toàn khỏe mạnh, đúng vào lúc hệ thống cần chúng nhất.

### Điều tôi đã đánh giá sai

Tôi tưởng phần khó nhất sẽ là hạ tầng — 14 container, Spark, Airflow. Thực tế hạ tầng chạy khá êm; thứ tốn thời gian là **những quyết định ngữ nghĩa nhỏ**: tie-break bằng `event_id` hay không, header rỗng hay vắng mặt, probe nào mandatory. Mỗi cái chỉ vài dòng code nhưng lại quyết định hệ thống hành xử ra sao lúc có sự cố.

Tôi cũng đánh giá thấp chi phí *vận hành* của môi trường: Docker engine chết một lần, và máy cạn paging file sau nhiều lần chạy full suite. Xem `docs/incident-note.md` sự cố 3.

### Điều sẽ cải tiến

Nếu làm lại, ba việc theo thứ tự ưu tiên:

1. **Cache kết quả probe của `/ready` với TTL 2–3s.** Đây là cải tiến có số đo hậu thuẫn rõ nhất: `/ready` fan-out tới 5 dependency mỗi lần gọi, và p50 tăng 487ms → 1211ms khi đi từ 8 lên 16 worker với 0 lỗi. Với `periodSeconds: 10` và HPA tối đa 8 replica thì đó là 8 vòng fan-out mỗi 10 giây chỉ để phục vụ probe. Một vòng fan-out dùng chung sẽ dời điểm bão hòa lên đáng kể mà không đổi ngữ nghĩa readiness.

2. **Alert theo tốc độ đốt error budget thay vì ngưỡng tĩnh.** `Lab28HighErrorRatio > 5% trong 2 phút` vừa ồn lúc traffic thấp (vài request lỗi thành tỉ lệ lớn) vừa điếc lúc traffic cao (5% của lưu lượng lớn là sự cố nghiêm trọng nhưng vẫn dưới ngưỡng). Đa cửa sổ 5m/1h và 6h/3d sửa được cả hai đầu.

3. **Tách plane theo vòng đời thay vì gom một compose.** Spark Connect (1.76 GiB) và MLflow (1.45 GiB) là hai container nặng nhất nhưng **không nằm trên đường request**. Gom chung khiến scale đường ask phải kéo theo cả Spark, và trên máy 15.7 GiB thì đó chính là nguyên nhân cạn RAM ở sự cố 3.

Ngoài ra, nếu có GPU tôi sẽ đóng nốt IP07 — đó là mảnh duy nhất còn thiếu, và đóng nó cũng tự động hoàn thiện IP10.

---

## 5. Đóng góp của thành viên

Làm **cá nhân** (nhánh `ca-nhan-hoang`), một người đảm nhiệm cả năm vai trò:

| Vai trò | Công việc |
|---|---|
| Ingestion & Orchestration | `event_headers`; xác minh trace qua Kafka; DAG run + asset event |
| Data & ML | `dedupe_latest`; `feast_online_request`; Delta MERGE/time travel; champion alias |
| Serving & Retrieval | `readiness_status`; degraded path; Qdrant deterministic ID |
| Platform & Observability | Gateway rate limit; Prometheus/Grafana; trace continuity; K8s/GitOps validation |
| Presenter / Incident Commander | Evidence pack; J4 failure/recovery; tài liệu này |

---

## 6. Cách tái lập

```powershell
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
docker compose --env-file ports.template --profile full up -d --build --wait

uv run lab28 topics
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed --via-gateway
uv run lab28 ready

uv run pytest starter-tests tests -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q

uv run lab28 evidence
uv run python load-tests/run_profile.py --requests 200 --workers 8
uv run python load-tests/run_profile.py --requests 200 --workers 16
uv run python scripts/validate_manifests.py
```

`evidence/` bị gitignore theo đúng quy định lab — nộp kèm riêng, không commit.

**Lưu ý khi demo recovery:** không dùng `docker compose down -v`. J4 dừng và khởi động lại service bằng `stop`/`start` để giữ nguyên volume; đó chính là điều khiến bằng chứng "không mất dữ liệu" (Kafka consumer lag = 0 sau toàn bộ suite) có ý nghĩa.

---

## 7. Bản đồ deliverable

Toàn bộ `evidence/` bị gitignore theo quy định lab — nộp kèm riêng, không commit.

| # | Deliverable (theo `SUBMISSION.md`) | File |
|---|---|---|
| 1 | Integration report | `evidence/integration-report.json` |
| 1 | Output fast suite | `evidence/fast-suite-output.txt` |
| 1 | Output live suite | `evidence/integration-suite-output.txt` |
| 2 | Evidence IP01–IP10 (11 file, IP09 có 2) | `evidence/ip01…ip10-*.json` |
| 3 | Architecture / ownership | `docs/architecture-ownership.md` + `docs/images/lab28-architecture-overview.svg` |
| 4 | Happy-path: run ID, trace ID, Delta version, MLflow version | `evidence/happy-path-trace.json` |
| 5 | Failure / recovery + no-data-loss proof | `docs/incident-note.md` (ghi chú) + `evidence/failure-recovery.json` (dữ liệu thô) |
| — | Replay-safe proof (IP03, J2) | `evidence/replay-safety.json` |
| 6 | Load profile P50/P95/P99 + bottleneck analysis | `evidence/load-profile.json` |
| 7 | Kubernetes / GitOps validation + drift/rollback | `evidence/gitops-validation.json` |
| 8 | Trade-offs, production gaps, đóng góp | `ANSWERS.md` (file này) |

Bốn định danh chính của happy path đã ghi nhận:

| Hạng mục | Giá trị |
|---|---|
| Airflow run ID | `it-4fe49c3d` |
| Trace ID | `68f76c6f81d64e7c8f8b8ac9776e1623` |
| Delta version (feedback) | v15 — 15 MERGE, time travel 0 → 25 rows |
| MLflow version | `lab28-rag-release` v5, alias `champion`, promoted from v2 |

### Đối chiếu với sáu mục nộp tối thiểu

| # | Yêu cầu | Nằm ở đâu |
|---|---|---|
| 1 | URL nhánh cá nhân trong repo private | nhánh `ca-nhan-hoang` — URL sau khi `git push -u origin ca-nhan-hoang` |
| 2 | Gói bằng chứng do `uv run lab28 evidence` tạo ra | `evidence/ip03`, `ip05`, `ip06`, `ip07`, `integration-report.json` (do CLI ghi) + `ip01`, `ip02`, `ip04`, `ip08`, `ip09`, `ip10` (do live test ghi) |
| 3 | Kết quả kiểm thử phần mã và integration matrix | `evidence/fast-suite-output.txt`, `evidence/integration-suite-output.txt`, `evidence/integration-report.json` |
| 4 | Chứng minh luồng đúng, replay-safe, metrics và trace | luồng: `evidence/happy-path-trace.json` · replay-safe: `evidence/replay-safety.json` · metrics: `evidence/ip09-*.json` · trace: `evidence/ip10-trace.json` |
| 5 | Ghi chú sự cố, dấu hiệu, nguyên nhân, khôi phục | `docs/incident-note.md` |
| 6 | Reflection: khó nhất, trade-off, sẽ cải tiến | `ANSWERS.md` §4b (khó nhất, sẽ cải tiến) và §1 (trade-off) |
| — | Làm cá nhân: đã đi qua các vai trò nào | `ANSWERS.md` §5 |

`docs/architecture-ownership.md` được **sinh tự động** từ `contracts/integration-matrix.yaml`, nên bảng owner/contract/evidence không thể lệch khỏi contract.

**Lưu ý khi demo recovery:** không dùng `docker compose down -v`. Cả J4 lẫn drill trong `failure-recovery.json` đều chỉ dùng `stop`/`start` để giữ nguyên volume; đó chính là điều khiến bằng chứng "không mất dữ liệu" có ý nghĩa.
