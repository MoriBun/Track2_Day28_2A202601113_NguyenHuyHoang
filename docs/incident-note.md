# Ghi chú sự cố — Day 28 Track 2

- **Người thực hiện:** Nguyễn Huy Hoàng — 2A202601113
- **Ngày:** 2026-09-03
- **Dữ liệu thô:** `evidence/failure-recovery.json`
- **Nguyên tắc:** chỉ dùng `docker compose stop` / `start`. Không `down -v`, vì `-v` xóa volume và sẽ phá chính thứ cần chứng minh là "không mất dữ liệu".

---

## Sự cố 1 — Feast down (chủ động inject, có kịch bản)

Đây là sự cố tôi cố ý tạo ra để kiểm chứng degraded policy.

### Giả thuyết trước khi inject

Feast là dependency **optional** trong `readiness_status`. Nếu thiết kế đúng thì mất Feast phải làm câu trả lời *tệ hơn*, không phải *sai*, nên `/ready` phải giữ **HTTP 200** với `status: degraded` và tuyệt đối không được rơi xuống `not_ready`/503 — vì 503 sẽ khiến gateway rút pod khỏi rotation và biến sự cố cục bộ thành mất dịch vụ toàn phần.

### Dấu hiệu quan sát

| Thời điểm (UTC) | Hành động | Quan sát |
|---|---|---|
| 10:19:54 | baseline | `status: degraded` (do vLLM vốn đã down), `feast: true` |
| 10:19:56 | `docker compose stop feast` | — |
| 10:20:00 | quan sát (~4s sau) | **HTTP 200**, `status: degraded`, `feast: false`, lý do `feast: unreachable: ConnectError` |
| 10:20:02 | `docker compose start feast` | — |
| 10:23:05 | quan sát | `feast: true` trở lại; `status` vẫn `degraded` **chỉ vì vLLM**, không phải vì Feast |

Component chuyển đúng `true → false → true`. Trong suốt sự cố `/ready` **chưa bao giờ** trả 503.

### Nguyên nhân

Nguyên nhân trực tiếp là do tôi chủ động dừng container. Điều đáng chú ý là **cách hệ thống phân loại** nguyên nhân đó: probe Feast được khai báo `mandatory=False`, nên `readiness_status` đi vào nhánh thứ hai của thứ tự ưu tiên (mandatory fail → `not_ready`; chỉ optional fail → `degraded`; còn lại → `ready`) và trả `degraded`.

Nếu Feast bị khai báo nhầm là mandatory, cùng một sự cố này sẽ khiến **toàn bộ** replica bị rút khỏi rotation cùng lúc. Đó là lý do việc phân loại probe quan trọng ngang với bản thân probe.

### Cách khôi phục và bằng chứng không mất dữ liệu

Khôi phục bằng `docker compose start feast`, không cần thao tác thủ công nào khác, không cần replay.

| Chỉ số | Trước | Sau | Kết luận |
|---|---|---|---|
| Qdrant points | 20 | 20 | không mất vector |
| Delta feedback version | v15 | v15 | không rollback, không mất commit |
| Consumer group `lab28-pipeline` / `data.raw` | lag 0 | lag 0 | không tồn đọng, không mất message |

**Về DLQ:** `lab28-pipeline-it-77fd04e4` trên `data.raw.dlq` còn lag 2 cả trước lẫn sau sự cố. Đây là các bản ghi độc mà một integration test cố ý đẩy vào dead letter queue, **không liên quan** đến sự cố này và **không phải mất dữ liệu**. Theo `runbooks/failure-injection.md`, DLQ chỉ được replay sau khi đã sửa nguyên nhân gốc, nên tồn đọng DLQ khác 0 là trạng thái nghỉ đúng thiết kế.

---

## Sự cố 2 — vLLM không tồn tại (điều kiện môi trường, không inject)

### Dấu hiệu

`POST /api/v1/ask` trả **HTTP 503** sau ~5.7 giây:

```json
{"category": "dependency_unavailable",
 "message": "inference endpoint unavailable: vLLM unreachable: ConnectError",
 "retryable": true, "trace_id": "16c8516fa4fd89ef258e5c0702321435"}
```

`lab28 ready` báo `vllm: not a verifiable vLLM server: unreachable: ConnectError`. Prometheus target `lab28-vllm-optional` (`host.docker.internal:8001`) ở `up=0`.

### Nguyên nhân

Máy chỉ có Intel Iris Xe tích hợp, không có GPU NVIDIA, `nvidia-smi` không tồn tại — nên `compose.gpu.yaml` không khởi động được, và lớp chưa cấp endpoint GPU thay thế.

Con số 5.7 giây là **connect timeout**, không phải độ trễ suy luận, nên tôi không dùng nó làm số SLO cho `/ask`.

### Cách xử lý

Không khôi phục được trên máy này, và tôi **không giả lập**. Một server giả OpenAI-compatible sẽ vượt được HTTP check nhưng cố ý trượt gate `gpu` trong `conftest.py`, vốn đòi `/version` trả build vLLM thật và `/metrics` có series tiền tố `vllm:`.

Hệ quả được báo cáo trung thực: IP07 = **UNVERIFIED**, và serving leg của IP10 (5 span sau `lab28.vllm.chat_completion`) bị gate theo. Khắc phục IP07 thì IP10 tự đầy đủ, không cần sửa dòng code nào.

Điểm tích cực: sự cố này cho thấy degraded policy hoạt động đúng ở tầng nghiêm trọng nhất — hệ thống trả 503 có `trace_id` và cờ `retryable` thay vì bịa ra câu trả lời không có grounding.

---

## Sự cố 3 — Docker engine chết giữa chừng (sự cố thật, ngoài kịch bản)

Đáng ghi lại vì đây là sự cố **thật**, không nằm trong kịch bản lab.

### Dấu hiệu

Mọi lệnh docker trả `request returned Internal Server Error ... /v1.46/containers/json`. Ứng dụng Docker Desktop vẫn chạy (4 process) nên thoạt nhìn tưởng bình thường.

### Nguyên nhân

`Get-Service com.docker.service` cho thấy service đặc quyền chạy engine đang **Stopped**, dù WSL distro `docker-desktop` vẫn Running. Ứng dụng chạy ≠ engine chạy.

Về sau còn gặp sự cố thứ hai liên quan: `cygheap read copy failed ... Win32 error 299` ở Git Bash và `The paging file is too small` khi PowerShell load assembly — máy cạn RAM/paging vì 14 container cộng nhiều lần chạy test trên 15.7 GiB.

### Cách khôi phục

Khởi động lại service ở quyền admin (`Start-Service com.docker.service`). Container quay lại đúng trạng thái `Exited (255)` rồi `start` lại được — **volume còn nguyên**, `.lab28/delta`, `.lab28/feast`, MLflow store đều không mất, nên toàn bộ evidence trước đó vẫn dùng được.

Với sự cố cạn RAM: `docker compose stop` (không `-v`) để giải phóng bộ nhớ. Bài học vận hành: dừng stack là thao tác an toàn, `down -v` mới là thao tác hủy — và trong một bài demo recovery thì nhầm hai lệnh này là mất trắng bằng chứng.

---

## Tóm tắt

| Sự cố | Dấu hiệu | Nguyên nhân | Khôi phục | Mất dữ liệu |
|---|---|---|---|---|
| Feast down (inject) | `/ready` 200 `degraded`, `feast: false` | probe optional → nhánh degraded | `start feast`, tự phục hồi | Không |
| vLLM vắng mặt | `/ask` 503 `dependency_unavailable` | không có GPU NVIDIA | không khôi phục được; báo UNVERIFIED | Không |
| Docker engine chết | mọi lệnh docker 500 | `com.docker.service` Stopped | start service ở quyền admin | Không |
