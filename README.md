# Đề Tài 11: Hệ Thống Gửi Thông Báo Hàng Loạt (Distributed Notification System)

Hệ thống gửi thông báo phân tán xây dựng trên nền tảng **C# / .NET 10**, tuân thủ các nguyên tắc thiết kế hệ thống phân tán chịu lỗi cao (**Fault-Tolerant Distributed Architecture**), xử lý bất đồng bộ (**Asynchronous Message-Driven**) qua **RabbitMQ**, lưu trữ dữ liệu tập trung với **PostgreSQL** & **Entity Framework Core**, cung cấp giao diện quản trị qua **Admin Console Client** và đóng gói hoàn chỉnh bằng **Docker & Docker Compose**.

---

## 1. Kiến Trúc & Công Nghệ Hệ Thống

### Sơ đồ kiến trúc phân tán

```
               ┌────────────────────────────────────────────────────────┐
               │              Admin.Client (.NET 10 Console)            │
               │         - Menu tương tác dòng lệnh trực quan           │
               │         - Giám sát tiến độ realtime, Retry             │
               └───────────────────────────┬────────────────────────────┘
                                           │ HTTP REST (Minimal API)
                                           ▼
               ┌────────────────────────────────────────────────────────┐
               │          Campaign.Api (ASP.NET Core Minimal API)       │
               │         - Tiếp nhận yêu cầu, quản lý chiến dịch        │
               │         - Tự động Migration / Retry khi khởi động     │
               │         - Sinh Delivery Job, Publish vào RabbitMQ      │
               └──────────────┬──────────────────────────┬──────────────┘
                              │                          │
                 Push Messages│ (AMQP)                   │ Direct DB Access
                              ▼                          │
         ┌───────────────────────────────────────┐       │
         │           RabbitMQ Broker             │       │
         │ - Queue: notification-jobs (durable)  │       │
         │ - DLQ:   notifications.dlq            │       │
         │ - Prefetch: 1 (Fair Dispatch)         │       │
         └─────────────┬───────────────────┬─────┘       │
                       │                   │             │
          Consume Job  │      Consume Job  │             │
          (Competing)  │      (Competing)  │             │
                       ▼                   ▼             │
        ┌───────────────────────┐ ┌───────────────────────┐
        │ Notification.Worker 1 │ │ Notification.Worker 2 │
        │   (Instance 1)        │ │   (Instance 2)        │
        │ - Idempotent Consumer │ │ - Idempotent Consumer │
        │ - Ack SAU KHI lưu DB  │ │ - Ack SAU KHI lưu DB  │
        │ - Auto Reconnect      │ │ - Auto Reconnect      │
        └──────────┬────────────┘ └──────────┬────────────┘
                   │                         │
                   │ Direct DB Access        │ Direct DB Access
                   ▼                         ▼
        ┌─────────────────────────────────────────────────┐
        │             PostgreSQL 16 Database              │
        │        - Campaigns, Recipients, Deliveries      │
        └─────────────────────────────────────────────────┘
```

### Công nghệ sử dụng
- **Nền tảng:** .NET 10 (`net10.0`), C# 13/14.
- **API Server:** ASP.NET Core Minimal API (tuyệt đối không dùng Controller MVC).
- **Message Broker:** RabbitMQ 3.x Management (thư viện `RabbitMQ.Client` 7.x async).
- **Cơ sở dữ liệu:** PostgreSQL 16 + Entity Framework Core 10 (`Npgsql.EntityFrameworkCore.PostgreSQL`).
- **Quản trị Client:** .NET 10 Console Application (`Admin.Client`).
- **Containerization:** Docker multi-stage builds, Docker Compose điều phối toàn bộ cụm dịch vụ.

---

## 2. Cấu Trúc Solution & Mã Nguồn

```
DistributedNotification/
├── DistributedNotification.sln         # Solution .NET 10 chuẩn
├── docker-compose.yml                  # Điều phối: postgres, rabbitmq, api, 2 workers
├── README.md                           # Tài liệu hướng dẫn & kịch bản kiểm thử
│
└── src/
    ├── Shared/                         # Class Library dùng chung
    │   ├── Shared.csproj
    │   ├── Entities/
    │   │   ├── Campaign.cs             # Entity chiến dịch (Id, Title, Content, Channel, Status...)
    │   │   ├── Recipient.cs            # Entity người nhận (Id, Name, Destination...)
    │   │   └── Delivery.cs             # Entity job gửi (Id, CampaignId, RecipientId, Status...)
    │   ├── Data/
    │   │   └── AppDbContext.cs         # EF Core DbContext + Fluent API cấu hình quan hệ
    │   ├── DTOs/
    │   │   └── Dtos.cs                 # DTOs Request/Response cho Minimal API & Client
    │   └── Messages/
    │       └── NotificationJobMessage.cs # Message contract gửi qua RabbitMQ
    │
    ├── Campaign.Api/                   # ASP.NET Core Minimal API
    │   ├── Campaign.Api.csproj
    │   ├── Program.cs                  # Khởi tạo DB retry, định nghĩa Minimal API Endpoints
    │   ├── Services/
    │   │   └── RabbitMqPublisher.cs    # RabbitMQ Producer (Async, Thread-Safe, DLQ setup)
    │   ├── appsettings.json
    │   └── Dockerfile
    │
    ├── Notification.Worker/            # .NET Worker Service (Chạy 2+ instances)
    │   ├── Notification.Worker.csproj
    │   ├── Program.cs                  # Host builder, DI, DB retry loop
    │   ├── Services/
    │   │   └── NotificationWorkerService.cs # BackgroundService: Consumer, Idempotency, Ack sau DB
    │   ├── appsettings.json
    │   └── Dockerfile
    │
    └── Admin.Client/                   # Console App giao diện dòng lệnh tương tác
        ├── Admin.Client.csproj
        ├── Program.cs                  # Menu CLI 9 chức năng, hiển thị tiến độ thời gian thực
        └── appsettings.json
```

---

## 3. Thiết Kế Cơ Sở Dữ Liệu (PostgreSQL + EF Core)

Hệ thống thiết kế tối thiểu 3 bảng quan hệ chặt chẽ:

1. **`Campaigns`**:
   - `Id` (Guid, PK): Khóa chính.
   - `Title` (varchar): Tiêu đề chiến dịch.
   - `Content` (text): Nội dung thông báo.
   - `Channel` (varchar): Kênh thông báo (`Email`, `SMS`, `Push`).
   - `Status` (varchar): Trạng thái (`Draft`, `Processing`, `Completed`).
   - `CreatedAt` (timestamp with time zone): Thời điểm tạo.

2. **`Recipients`**:
   - `Id` (Guid, PK): Khóa chính.
   - `Name` (varchar): Họ tên người nhận.
   - `Destination` (varchar): Địa chỉ nhận (Email hoặc SĐT hoặc Device Token).
   - `CreatedAt` (timestamp with time zone): Thời điểm tạo.

3. **`Deliveries`** (đại diện cho từng job gửi thông báo):
   - `Id` (Guid, PK): Khóa chính job.
   - `CampaignId` (Guid, FK): Khóa ngoại tham chiếu `Campaigns.Id` (Cascade delete).
   - `RecipientId` (Guid, FK): Khóa ngoại tham chiếu `Recipients.Id` (Cascade delete).
   - `Status` (varchar): Trạng thái gửi (`Pending`, `Sent`, `Failed`).
   - `RetryCount` (int): Số lần đã thử lại (mặc định 0).
   - `ErrorMessage` (text, nullable): Chi tiết thông báo lỗi khi thất bại.
   - `WorkerId` (varchar, nullable): Tên container worker đã xử lý job (ví dụ: `notification-worker-1`).
   - `CorrelationId` (varchar): Định danh phân tán để truy vết luồng xử lý xuyên suốt.
   - `UpdatedAt` (timestamp with time zone): Thời điểm cập nhật cuối cùng.

> **Tự phục hồi kết nối DB khi khởi động:** Cả `Campaign.Api` và `Notification.Worker` đều tích hợp sẵn vòng lặp retry 15 lần (khoảng cách 3 giây) khi thực hiện `EnsureCreatedAsync()`, tránh việc service bị crash khi container PostgreSQL khởi động chậm hơn.

---

## 4. Danh Sách Endpoint REST API (`Campaign.Api`)

Tất cả endpoints triển khai theo kiến trúc **ASP.NET Core Minimal API**:

| Phương thức | Endpoint | Mô tả | Mã HTTP trả về |
|---|---|---|---|
| `POST` | `/campaigns` | Tạo chiến dịch mới kèm danh sách người nhận (hỗ trợ nhập mới hoặc chọn người nhận có sẵn). Tự động tạo các bản ghi `Deliveries` ở trạng thái `Pending`. | `201 Created` |
| `GET` | `/campaigns` | Lấy danh sách toàn bộ chiến dịch kèm thống kê tổng số lượng `Pending`, `Sent`, `Failed`. | `200 OK` |
| `GET` | `/campaigns/{id}` | Lấy thông tin chi tiết một chiến dịch và số liệu thống kê thời gian thực. | `200 OK` / `404 Not Found` |
| `POST` | `/campaigns/{id}/start` | Chuyển trạng thái sang `Processing`, sinh message cho các job `Pending` và đẩy bất đồng bộ vào RabbitMQ. Trả về ngay lập tức không bắt client chờ. | `202 Accepted` |
| `GET` | `/campaigns/{id}/deliveries` | Tra cứu chi tiết kết quả gửi của từng người nhận: trạng thái, WorkerId xử lý, số lần retry, mã lỗi. | `200 OK` |
| `POST` | `/campaigns/{id}/retry` | Chạy lại các job `Failed`: đưa trạng thái về `Pending`, tăng `RetryCount`, xóa lỗi cũ và đẩy lại vào RabbitMQ. | `202 Accepted` |
| `GET` | `/health` | Kiểm tra trạng thái kết nối tới PostgreSQL và RabbitMQ. | `200 OK` (Healthy) / `503 Service Unavailable` |
| `GET` | `/recipients` | Tra cứu danh sách người nhận đã lưu trong hệ thống để tái sử dụng. | `200 OK` |

---

## 5. Cơ Chế Xử Lý Phân Tán Của `Notification.Worker`

1. **Phân phối công bằng (Fair Dispatch - Competing Consumers):**
   - Queue `notification-jobs` khai báo với cờ `durable: true`, message `persistent: true`.
   - Thiết lập `channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false)` giúp RabbitMQ chỉ phát 1 message cho mỗi worker tại một thời điểm. Worker nào rảnh sẽ nhận message tiếp theo, tự động cân bằng tải dù tốc độ xử lý giữa các worker khác nhau.
2. **Khả năng chịu lỗi & Không mất mát dữ liệu (Fault Tolerance):**
   - Chỉ gọi `channel.BasicAckAsync()` **sau khi** kết quả xử lý (`Sent` hoặc `Failed`) đã được `db.SaveChangesAsync()` thành công vào PostgreSQL.
   - Nếu worker bị sập (crash / `docker stop`) giữa lúc đang xử lý, kết nối TCP bị ngắt, RabbitMQ sẽ **tự động requeue** message đó và chuyển giao ngay cho worker còn lại xử lý.
3. **Idempotent Consumer (Chống gửi trùng lặp):**
   - Trước khi thực hiện gửi, Worker truy vấn DB kiểm tra: Nếu `delivery.Status == "Sent"`, worker lập tức log ghi nhận và gọi `BasicAckAsync` bỏ qua, đảm bảo thông báo không bao giờ bị gửi hai lần.
4. **Dead-Letter Queue (DLQ):**
   - Queue chính được cấu hình tham số `x-dead-letter-exchange: ""` và `x-dead-letter-routing-key: "notifications.dlq"`.
   - Các message bị lỗi định dạng hoặc lỗi không thể khắc phục được `BasicNackAsync(requeue: false)` và tự động chuyển sang hàng đợi `notifications.dlq` để bảo toàn dữ liệu phục vụ điều tra.
5. **Mô phỏng gửi thực tế & Tỷ lệ lỗi kiểm thử:**
   - Mỗi job được giả lập độ trễ mạng từ **300ms đến 500ms**.
   - Nếu địa chỉ nhận chứa từ khóa `fail`, worker sẽ đánh dấu `Failed` có kiểm soát.
   - Với các job lần đầu (`RetryCount == 0`), worker áp dụng tỷ lệ lỗi ngẫu nhiên ~15% để demo cơ chế Retry.
6. **Tự động kết nối lại (Auto Reconnect):**
   - Vòng lặp `while (!stoppingToken.IsCancellationRequested)` bao bọc kết nối RabbitMQ. Nếu broker bị khởi động lại hoặc mạng gián đoạn, worker sẽ tự động kết nối lại sau 5 giây.
7. **Structured Logging & Correlation ID:**
   - Log ghi nhận chi tiết: `[{WorkerId}] [{CorrelationId}] Processing Delivery {DeliveryId} | Campaign {CampaignId} -> {Destination}`.

---

## 6. Hướng Dẫn Cài Đặt & Khởi Chạy

### Yêu cầu tiên quyết
- Đã cài đặt [Docker](https://www.docker.com/) & Docker Compose.
- [.NET 10 SDK](https://dotnet.microsoft.com/) (để chạy `Admin.Client` trên máy host).

### Bước 1: Khởi động hệ thống bằng Docker Compose
Mở PowerShell hoặc Terminal tại thư mục gốc của dự án (`Hệ thống`):

```powershell
docker-compose up --build -d
```

Lệnh trên sẽ tự động:
1. Tải và khởi chạy PostgreSQL 16 trên cổng `5432`.
2. Tải và khởi chạy RabbitMQ 3 Management trên cổng `5672` (AMQP) và `15672` (Web UI).
3. Build và khởi chạy `campaign-api` trên cổng `5000` (ánh xạ vào cổng `8080` của container).
4. Build và khởi chạy 2 container worker độc lập: `notification-worker-1` và `notification-worker-2`.

### Bước 2: Kiểm tra trạng thái các container
```powershell
docker-compose ps
```

Xem log khởi động của API:
```powershell
docker-compose logs -f campaign-api
```
Chờ thông báo: `Database initialized successfully.` và `Connected to RabbitMQ`.

Kiểm tra RabbitMQ Management Dashboard:
- Truy cập trình duyệt: [http://localhost:15672](http://localhost:15672)
- Tài khoản: `guest` / Mật khẩu: `guest`
- Bạn sẽ thấy queue `notification-jobs` và `notifications.dlq` đã được khai báo sẵn sàng.

### Bước 3: Khởi chạy ứng dụng Admin Client
Mở một cửa sổ Terminal/PowerShell mới:

```powershell
cd "c:\Users\Dell Precision 5550\Documents\Hệ thống"
dotnet run --project src/Admin.Client
```

> **Lưu ý cấu hình URL:** Mặc định client kết nối tới `http://localhost:5000`. Bạn có thể chỉ định URL tùy ý qua biến môi trường:
> ```powershell
> $env:API_BASE_URL="http://localhost:5000"
> dotnet run --project src/Admin.Client
> ```

---

## 7. Hướng Dẫn Sử Dụng Giao Diện Dòng Lệnh (`Admin.Client`)

Menu chính của ứng dụng gồm 9 chức năng trực quan:

```
╔═══════════════════════════════════════════════════════════════════╗
║        HỆ THỐNG GỬI THÔNG BÁO HÀNG LOẠT (ĐỀ TÀI 11)               ║
║            Distributed Notification System - Admin Client         ║
╠═══════════════════════════════════════════════════════════════════╣
║  1. Tạo chiến dịch mẫu (nhanh 20-50 người nhận để test)           ║
║  2. Tạo chiến dịch thủ công (nhập mới hoặc chọn người nhận)       ║
║  3. Xem danh sách chiến dịch                                      ║
║  4. Xem chi tiết chiến dịch                                       ║
║  5. Kích hoạt chiến dịch (Start - Đẩy vào RabbitMQ)               ║
║  6. Xem chi tiết kết quả gửi (Deliveries)                         ║
║  7. Chạy lại các job thất bại (Retry Failed Jobs)                 ║
║  8. Theo dõi tiến độ thời gian thực (Real-time Monitor)           ║
║  9. Kiểm tra sức khỏe hệ thống (Health Check)                     ║
║  0. Thoát                                                         ║
╚═══════════════════════════════════════════════════════════════════╝
```

### Luồng thao tác chuẩn để demo nhanh:
1. **Bấm phím `1`:** Nhập số lượng người nhận mẫu (ví dụ: `30`), chọn kênh (`Email`). Hệ thống tự động tạo 30 người nhận, sinh sẵn 30 bản ghi Deliveries trạng thái `Pending`. Lưu lại `Campaign ID`.
2. **Bấm phím `5`:** Nhập `Campaign ID`, API lập tức đổi trạng thái sang `Processing`, đẩy 30 job vào RabbitMQ và trả về `202 Accepted`.
3. **Bấm phím `8`:** Nhập `Campaign ID` để mở màn hình giám sát tiến độ thời gian thực. Quan sát thanh tiến độ `[████████████░░░░]` tăng dần kèm số lượng `Pending`, `Sent`, `Failed`.
4. **Bấm phím `6`:** Xem bảng kết quả chi tiết từng người nhận, nhận thấy cả hai worker `notification-worker-1` và `notification-worker-2` cùng chia sẻ xử lý các job.
5. **Bấm phím `7`:** Chạy lại các job lỗi nếu có. Sau đó quay lại `8` để theo dõi tiến độ retry.
6. **Bấm phím `9`:** Kiểm tra nhanh trạng thái PostgreSQL và RabbitMQ (`Healthy`).

---

## 8. Kịch Bản Kiểm Thử Khả Năng Chịu Lỗi (Fault Tolerance Scenarios)

Dưới đây là 3 kịch bản kiểm thử mẫu để nghiệm thu và chấm điểm hệ thống phân tán:

### Kịch bản 1: Kiểm thử cân bằng tải song song giữa 2 Worker
1. Tạo chiến dịch mẫu với 30 người nhận (Menu `1`).
2. Kích hoạt chiến dịch (Menu `5`).
3. Mở xem log song song của cả hai worker:
   ```powershell
   docker-compose logs -f notification-worker-1 notification-worker-2
   ```
4. **Kết quả quan sát được:** Các dòng log xử lý xuất hiện xen kẽ giữa `[notification-worker-1]` và `[notification-worker-2]`. Khi vào Menu `6`, cột `Worker` trong cơ sở dữ liệu hiển thị chia đều cho cả hai worker.

### Kịch bản 2: Kiểm thử dừng đột ngột 1 Worker (Worker Crash & Message Requeue)
1. Tạo một chiến dịch lớn gồm 50 người nhận (Menu `1`).
2. Kích hoạt chiến dịch (Menu `5`).
3. Ngay khi hệ thống bắt đầu gửi, lập tức dừng đột ngột container `notification-worker-1`:
   ```powershell
   docker stop hethong-notification-worker-1-1
   # hoặc: docker-compose stop notification-worker-1
   ```
4. Quan sát log của worker còn lại:
   ```powershell
   docker-compose logs -f notification-worker-2
   ```
5. **Kết quả quan sát được:**
   - Worker 2 tiếp tục nhận và xử lý toàn bộ các job còn lại mà không có bất kỳ thông báo nào bị mất hoặc bị kẹt.
   - Job mà Worker 1 đang xử lý dở dang (chưa gọi `BasicAck`) được RabbitMQ tự động requeue và Worker 2 tiếp nhận xử lý thành công.
   - Không có bản ghi nào bị rơi vào trạng thái mất mát.

### Kịch bản 3: Kiểm thử cơ chế Retry các job thất bại
1. Trong danh sách người nhận mẫu, có các địa chỉ chứa từ khóa `fail` (ví dụ: `fail_user_08@notification-demo.com`).
2. Sau khi chiến dịch hoàn tất, xem chi tiết (Menu `4` hoặc `6`), nhận thấy có các job ở trạng thái `Failed` với nguyên nhân lỗi được ghi rõ tại `ErrorMessage`.
3. Chọn Menu `7` (Retry Failed Jobs) và nhập `Campaign ID`.
4. **Kết quả quan sát được:** Các job lỗi được chuyển lại trạng thái `Pending`, `RetryCount` tăng từ `0` lên `1`, message được đẩy lại vào RabbitMQ và các worker tiếp tục xử lý lại.

### Kịch bản 4: Kiểm thử tạm ngừng RabbitMQ (Broker Reconnect)
1. Dừng container RabbitMQ:
   ```powershell
   docker-compose stop rabbitmq
   ```
2. Quan sát log các worker: Các worker liên tục ghi nhận cảnh báo mất kết nối và tự động thử kết nối lại mỗi 5 giây (`Reconnecting in 5 seconds...`).
3. Khởi động lại RabbitMQ:
   ```powershell
   docker-compose start rabbitmq
   ```
4. **Kết quả quan sát được:** Cả API và các Worker tự động khôi phục kết nối AMQP thành công, sẵn sàng tiếp tục nhận lệnh mà không cần restart lại ứng dụng .NET.

---

## 9. Cấu Hình Biến Môi Trường (Environment Variables)

Hệ thống tuân thủ nguyên tắc cấu hình linh hoạt (12-Factor App):

| Biến môi trường | Dịch vụ sử dụng | Giá trị mẫu | Mục đích |
|---|---|---|---|
| `ConnectionStrings__DefaultConnection` | `Campaign.Api`, `Notification.Worker` | `Host=postgres;Database=notifications;Username=postgres;Password=postgres` | Chuỗi kết nối PostgreSQL |
| `RabbitMQ__HostName` | `Campaign.Api`, `Notification.Worker` | `rabbitmq` (trong Docker) hoặc `localhost` | Địa chỉ máy chủ RabbitMQ |
| `RabbitMQ__UserName` | `Campaign.Api`, `Notification.Worker` | `guest` | Tên đăng nhập RabbitMQ |
| `RabbitMQ__Password` | `Campaign.Api`, `Notification.Worker` | `guest` | Mật khẩu RabbitMQ |
| `WORKER_ID` | `Notification.Worker` | `notification-worker-1` / `worker-2` | Định danh phân biệt từng instance worker |
| `API_BASE_URL` | `Admin.Client` | `http://localhost:5000` | URL gốc của Campaign API |

---

## 10. Lệnh Dừng & Thu Dọn Tài Nguyên

Khi muốn dừng hệ thống:
```powershell
docker-compose down
```

Nếu muốn xóa toàn bộ dữ liệu database để chạy lại từ đầu:
```powershell
docker-compose down -v
```
