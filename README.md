# Đề Tài 11: Hệ Thống Gửi Thông Báo Hàng Loạt (Distributed Notification System)

Hệ thống gửi thông báo phân tán xây dựng trên nền tảng **C# / .NET 10**, tuân thủ các nguyên tắc thiết kế hệ thống phân tán chịu lỗi cao (**Fault-Tolerant Distributed Architecture**), xử lý bất đồng bộ (**Asynchronous Message-Driven**) qua **RabbitMQ**, lưu trữ dữ liệu tập trung với **PostgreSQL** & **Entity Framework Core**, cập nhật tiến độ thời gian thực qua **SignalR**, cung cấp giao diện quản trị Web hiện đại (**Blazor Web App**) lẫn Console App (**Admin.Client**) và đóng gói hoàn chỉnh bằng **Docker & Docker Compose**.

---

## 1. Kiến Trúc & Công Nghệ Hệ Thống

### Sơ đồ kiến trúc phân tán & Luồng giao tiếp

```
      ┌───────────────────────────┐         ┌───────────────────────────┐
      │  Admin.Web (Blazor .NET10)│         │ Admin.Client (.NET10 CLI) │
      │  - Giao diện Web hiện đại │         │ - Menu tương tác console  │
      │  - SignalR Real-time      │         │ - Theo dõi tiến độ CLI    │
      └─────────────┬─────────────┘         └─────────────┬─────────────┘
                    │                                     │
                    │ HTTP REST & SignalR WebSocket       │ HTTP REST
                    ▼                                     ▼
      ┌─────────────────────────────────────────────────────────────────┐
      │             Campaign.Api (ASP.NET Core Minimal API)             │
      │        - Tiếp nhận yêu cầu, quản lý chiến dịch thông báo        │
      │        - SignalR Hub: /hubs/campaign (Realtime broadcast)       │
      │        - Tự động Migration / Retry khi PostgreSQL khởi động     │
      │        - Sinh Delivery Job, Publish vào RabbitMQ                │
      └───────────────┬─────────────────────────────────┬───────────────┘
                      │                                 │
         Push Messages│ (AMQP)                          │ Direct DB Access
                      ▼                                 │
         ┌──────────────────────────────────────┐       │
         │           RabbitMQ Broker            │       │
         │ - Queue: notification-jobs (durable) │       │
         │ - DLQ:   notifications.dlq           │       │
         │ - Prefetch: 1 (Fair Dispatch)        │       │
         └────────────┬───────────────────┬─────┘       │
                      │                   │             │
         Consume Job  │      Consume Job  │             │
         (Competing)  │      (Competing)  │             │
                      ▼                   ▼             │
       ┌───────────────────────┐ ┌───────────────────────┐
       │ Notification.Worker 1 │ │ Notification.Worker 2 │
       │   (Instance 1)        │ │   (Instance 2)        │
       │ - Idempotent Consumer │ │ - Idempotent Consumer │
       │ - Ack SAU KHI lưu DB  │ │ - Ack SAU KHI lưu DB  │
       │ - Notify SignalR API  │ │ - Notify SignalR API  │
       └──────────┬────────────┘ └──────────┬────────────┘
                  │                         │
                  │ Direct DB Access        │ Direct DB Access
                  ▼                         ▼
       ┌─────────────────────────────────────────────────┐
       │             PostgreSQL 16 Database              │
       │        - Campaigns, Recipients, Deliveries      │
       └─────────────────────────────────────────────────┘
```

### Luồng tương tác Real-time giữa các thành phần:
1. **Khởi tạo & Kích hoạt:** Người dùng tạo và bấm "Start" chiến dịch trên **Admin.Web** (hoặc **Admin.Client**).
2. **Hàng đợi thông điệp:** `Campaign.Api` lưu trạng thái `Processing` vào PostgreSQL và đẩy các job `Pending` vào RabbitMQ.
3. **Cạnh tranh xử lý (Competing Consumers):** Các container `notification-worker-1` và `notification-worker-2` cùng lắng nghe queue với cơ chế `prefetch: 1`.
4. **Cập nhật & Báo hiệu Real-time:** Ngay sau khi Worker cập nhật trạng thái `Sent` hoặc `Failed` vào PostgreSQL và gọi `BasicAck`, Worker gọi endpoint `POST /internal/campaigns/{id}/notify-progress` của API.
5. **SignalR Broadcast:** `Campaign.Api` truy vấn số liệu mới nhất và phát sóng qua `CampaignHub` tới các trình duyệt web đang mở. Giao diện Blazor tự động cập nhật thanh tiến độ (Progress Bar) và các con số thống kê theo thời gian thực mà không cần reload trang.

---

## 2. Cấu Trúc Solution & Mã Nguồn

```
DistributedNotification/
├── DistributedNotification.sln          # Solution .NET 10 quản lý 5 projects
├── docker-compose.yml                   # Khởi chạy: postgres, rabbitmq, api, 2 workers, web
├── README.md                            # Tài liệu hướng dẫn chi tiết
│
└── src/
    ├── Shared/                          # Class Library (net10.0) dùng chung
    │   ├── Shared.csproj
    │   ├── Entities/
    │   │   ├── Campaign.cs              # Entity Campaigns (Id, Title, Content, Channel, Status, CreatedAt)
    │   │   ├── Recipient.cs             # Entity Recipients (Id, Name, Destination, CreatedAt)
    │   │   └── Delivery.cs              # Entity Deliveries (Id, CampaignId, RecipientId, Status, RetryCount, ErrorMessage, WorkerId, CorrelationId, UpdatedAt)
    │   ├── Data/
    │   │   └── AppDbContext.cs          # EF Core DbContext + Fluent API cấu hình quan hệ & Indexes
    │   ├── DTOs/
    │   │   └── Dtos.cs                  # Data Transfer Objects
    │   └── Messages/
    │       └── NotificationJobMessage.cs# Message Contract cho RabbitMQ
    │
    ├── Campaign.Api/                    # ASP.NET Core Minimal API + SignalR (net10.0)
    │   ├── Hubs/
    │   │   └── CampaignHub.cs           # SignalR Hub: join/leave group theo campaignId
    │   ├── Services/
    │   │   └── RabbitMqPublisher.cs     # RabbitMQ Producer (Async, Thread-Safe, DLQ setup)
    │   ├── Program.cs                   # Minimal API endpoints + SignalR broadcast + DB retry loop
    │   ├── appsettings.json
    │   └── Dockerfile
    │
    ├── Notification.Worker/             # .NET 10 Worker Service (BackgroundService)
    │   ├── Services/
    │   │   └── NotificationWorkerService.cs # Consumer, Idempotency, Ack sau DB, Trigger SignalR API
    │   ├── Program.cs                   # Host builder, DI, DB retry loop
    │   ├── appsettings.json
    │   └── Dockerfile
    │
    ├── Admin.Web/                       # Website Quản Trị Blazor Web App (net10.0)
    │   ├── Components/
    │   │   ├── Pages/
    │   │   │   ├── Dashboard.razor      # Dashboard tổng quan, KPI, bảng chiến dịch, live SignalR
    │   │   │   ├── CreateCampaign.razor # Form tạo chiến dịch (Sinh mẫu test / Nhập mới / Chọn có sẵn)
    │   │   │   └── CampaignDetail.razor # Giám sát realtime, Progress bar, lọc kết quả, badge Worker
    │   │   ├── Layout/
    │   │   │   └── MainLayout.razor     # Navbar, Health Check status pill, footer
    │   │   ├── App.razor                # Bootstrap 5.3 + Bootstrap Icons
    │   │   └── Routes.razor
    │   ├── Services/
    │   │   └── CampaignApiClient.cs     # Client gọi REST API của Campaign.Api
    │   ├── Program.cs                   # Blazor Interactive Server + HttpClient configuration
    │   ├── appsettings.json
    │   └── Dockerfile
    │
    └── Admin.Client/                    # Console App giao diện dòng lệnh tương tác (.NET 10)
        ├── Program.cs                   # Menu CLI 9 chức năng, hiển thị tiến độ thời gian thực
        └── appsettings.json
```

---

## 3. Danh Sách Endpoint REST API & SignalR Hub (`Campaign.Api`)

| Phương thức | Endpoint | Mô tả |
|---|---|---|
| `GET / WebSocket` | `/hubs/campaign` | **SignalR Hub** cho client kết nối nhận sự kiện tiến độ (`CampaignProgressUpdated`, `GlobalProgressUpdated`). |
| `POST` | `/campaigns` | Tạo chiến dịch mới kèm người nhận (sinh sẵn `Deliveries` trạng thái `Pending`). |
| `GET` | `/campaigns` | Lấy danh sách toàn bộ chiến dịch kèm thống kê `Pending`, `Sent`, `Failed`. |
| `GET` | `/campaigns/{id}` | Lấy chi tiết chiến dịch và số liệu thống kê thời gian thực. |
| `POST` | `/campaigns/{id}/start` | Đổi trạng thái sang `Processing`, đẩy job vào RabbitMQ, trả về `202 Accepted` ngay lập tức. |
| `GET` | `/campaigns/{id}/deliveries`| Tra cứu chi tiết kết quả gửi: trạng thái, WorkerId, RetryCount, ErrorMessage. |
| `POST` | `/campaigns/{id}/retry` | Chạy lại các job `Failed`: đưa về `Pending`, tăng `RetryCount` và đẩy lại vào RabbitMQ. |
| `POST` | `/internal/campaigns/{id}/notify-progress` | Endpoint nội bộ để Worker gọi kích hoạt SignalR broadcast tiến độ mới nhất. |
| `GET` | `/health` | Kiểm tra trạng thái kết nối tới PostgreSQL và RabbitMQ (`Healthy` / `Unhealthy`). |
| `GET` | `/recipients` | Danh sách người nhận đã lưu để tái sử dụng. |

---

## 4. Hướng Dẫn Cài Đặt & Khởi Chạy

### Yêu cầu tiên quyết
- Đã cài đặt [Docker](https://www.docker.com/) & Docker Compose.
- [.NET 10 SDK](https://dotnet.microsoft.com/) (nếu muốn chạy thử nghiệm trực tiếp trên máy host).

### Cách 1: Khởi chạy toàn bộ hệ thống bằng Docker Compose (Khuyên dùng)
Tại thư mục gốc của dự án (`c:\Users\Dell Precision 5550\Documents\Hệ thống`):

```powershell
docker-compose up --build -d
```

Docker Compose sẽ tự động khởi chạy 6 containers:
1. `postgres` (cổng `5432`)
2. `rabbitmq` (cổng `5672` và Management UI `15672`)
3. `campaign-api` (cổng `5000:8080`)
4. `notification-worker-1` (instance Worker #1)
5. `notification-worker-2` (instance Worker #2)
6. `admin-web` (cổng `5001:8080`)

#### Truy cập các giao diện:
- **🌐 Website Quản Trị (Blazor Web App):** [http://localhost:5001](http://localhost:5001)
- **⚡ REST API & SignalR Hub:** [http://localhost:5000](http://localhost:5000)
- **🏥 API Health Check:** [http://localhost:5000/health](http://localhost:5000/health)
- **🐇 RabbitMQ Dashboard:** [http://localhost:15672](http://localhost:15672) (User: `guest` / Pass: `guest`)

---

### Cách 2: Chạy song song Admin Client (Console App)
Mở một cửa sổ Terminal mới:

```powershell
dotnet run --project src/Admin.Client
```

---

## 5. Hướng Dẫn Sử Dụng Giao Diện Web Quản Trị (`Admin.Web`)

Truy cập [http://localhost:5001](http://localhost:5001) trên trình duyệt:

### 1. Màn hình Dashboard (`/`)
- **Các thẻ KPI:** Tổng số chiến dịch, Tổng thông báo Thành công (Sent), Đang chờ (Pending), Thất bại (Failed).
- **Trạng thái SignalR Live:** Huy hiệu `● SignalR Trực tiếp` hiển thị trạng thái kết nối socket.
- **Trạng thái Hạ tầng (Health Check):** Hiển thị rõ PostgreSQL (`Connected`) và RabbitMQ (`Connected`).
- **Bảng chiến dịch:** Xem danh sách, tìm kiếm, xem thanh tiến độ nhỏ và các nút thao tác nhanh:
  - Nút **Bắt đầu (Start)**: Kích hoạt chiến dịch `Draft`.
  - Nút **Thử lại (Retry)**: Chạy lại job lỗi nếu có.
  - Nút **Chi tiết**: Mở trang giám sát chi tiết.

### 2. Màn hình Tạo chiến dịch mới (`/campaigns/create`)
- Nhập Tiêu đề, Nội dung thông báo, Kênh gửi (`Email`, `SMS`, `Push`).
- Cung cấp **3 lựa chọn thêm người nhận linh hoạt**:
  - **(a) Sinh Mẫu Dữ Liệu Test:** Cho phép chọn từ 10 đến 50 người nhận mẫu. Tự động chèn ~10% địa chỉ lỗi chứa từ khóa `fail` và các địa chỉ ngẫu nhiên thất bại ở lần đầu để kiểm thử cơ chế Retry & DLQ.
  - **(b) Nhập Thủ Công Mới:** Thêm từng dòng Họ tên và Địa chỉ nhận.
  - **(c) Chọn Người Nhận Có Sẵn:** Tải từ DB, hỗ trợ chọn tất cả hoặc tích chọn từng người nhận.
- Bấm "Tạo Chiến Dịch" để lưu và chuyển ngay đến trang giám sát.

### 3. Màn hình Chi tiết & Giám sát Real-time (`/campaigns/{id}`)
- **SignalR Real-time:** Khi Worker xử lý xong từng job, thanh tiến độ và các con số `Pending`, `Sent`, `Failed` nhảy số tự động ngay trên màn hình mà **không cần reload trang**.
- **Thao tác trực tiếp:**
  - Nút **🚀 Kích hoạt gửi (Start)** để đẩy job vào RabbitMQ.
  - Nút **🔄 Chạy lại job lỗi (Retry Failed)** để reprocess các job thất bại.
- **Bảng Deliveries chi tiết:**
  - Bộ lọc theo trạng thái: `Tất cả`, `Sent`, `Failed`, `Pending`.
  - Tìm kiếm theo tên hoặc địa chỉ nhận.
  - **Phân biệt Worker:** Cột "Worker Xử Lý" gắn badge màu riêng biệt cho `notification-worker-1` (Xanh dương) và `notification-worker-2` (Tím), chứng minh rõ ràng cả 2 worker cùng tham gia xử lý song song.
  - Hiển thị số lần Retry, Correlation ID và chi tiết lỗi nếu job thất bại.

---

## 6. Kịch Bản Kiểm Thử Khả Năng Chịu Lỗi (Fault Tolerance)

### Kịch bản 1: Giám sát cân bằng tải song song trên giao diện Web
1. Truy cập [http://localhost:5001](http://localhost:5001), vào "Tạo chiến dịch mới".
2. Chọn tab "(a) Sinh Mẫu Dữ Liệu Test" với số lượng `30` người nhận.
3. Bấm "Tạo Chiến Dịch", sau đó bấm "🚀 Kích hoạt gửi (Start)".
4. **Quan sát:**
   - Thanh tiến độ trên Web tự động chạy từ 0% đến 100% qua SignalR.
   - Bảng kết quả bên dưới hiển thị cột "Worker Xử Lý" có sự xuất hiện xen kẽ của cả `notification-worker-1` và `notification-worker-2`.

### Kịch bản 2: Dừng đột ngột 1 Worker khi đang gửi (Worker Crash)
1. Tạo chiến dịch mẫu với `40` người nhận và bấm "Start".
2. Trong khi thanh tiến độ đang chạy, mở PowerShell dừng ngay `notification-worker-1`:
   ```powershell
   docker-compose stop notification-worker-1
   ```
3. **Quan sát trên Web:**
   - Thanh tiến độ không bị dừng lại hay treo. `notification-worker-2` tiếp tục nhận và hoàn thành tất cả các job còn lại.
   - Các job chưa kịp `Ack` từ worker 1 được RabbitMQ tự động chuyển cho worker 2. Không có bất kỳ thông báo nào bị mất.

### Kịch bản 3: Kiểm thử Retry job thất bại qua Web UI
1. Sau khi chiến dịch hoàn tất, có các job mang trạng thái `Failed` do địa chỉ mẫu chứa từ khóa `fail`.
2. Trên giao diện Web, bấm nút "🔄 Chạy lại job lỗi".
3. **Quan sát:**
   - Trạng thái chiến dịch chuyển về `Processing`.
   - Các job lỗi được chuyển về `Pending`, `RetryCount` tăng lên `1`, lỗi cũ được xóa và các worker tiếp tục xử lý lại ngay lập tức.

---

## 7. Cấu Hình Biến Môi Trường

| Biến môi trường | Áp dụng cho | Giá trị trong Docker |
|---|---|---|
| `ConnectionStrings__DefaultConnection` | `Campaign.Api`, `Notification.Worker` | `Host=postgres;Database=notifications;Username=postgres;Password=postgres` |
| `RabbitMQ__HostName` | `Campaign.Api`, `Notification.Worker` | `rabbitmq` |
| `RabbitMQ__UserName` | `Campaign.Api`, `Notification.Worker` | `guest` |
| `RabbitMQ__Password` | `Campaign.Api`, `Notification.Worker` | `guest` |
| `CampaignApiUrl` | `Notification.Worker`, `Admin.Web` | `http://campaign-api:8080` |
| `SignalRHubUrl` | `Admin.Web` | `http://campaign-api:8080/hubs/campaign` |
| `API_BASE_URL` | `Admin.Client` | `http://localhost:5000` |
