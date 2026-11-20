using System.Net.Http.Json;
using System.Text.Json;

// ── Configuration ──
var baseUrl = Environment.GetEnvironmentVariable("API_BASE_URL") ?? "http://localhost:5000";
using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };
client.Timeout = TimeSpan.FromSeconds(30);
var jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

try
{
    Console.OutputEncoding = System.Text.Encoding.UTF8;
}
catch { }

while (true)
{
    try
    {
        if (!Console.IsOutputRedirected)
        {
            Console.Clear();
        }
    }
    catch { }

    Console.ForegroundColor = ConsoleColor.Cyan;
    Console.WriteLine("╔═══════════════════════════════════════════════════════════════════╗");
    Console.WriteLine("║        HỆ THỐNG GỬI THÔNG BÁO HÀNG LOẠT (ĐỀ TÀI 11)               ║");
    Console.WriteLine("║            Distributed Notification System - Admin Client         ║");
    Console.WriteLine("╠═══════════════════════════════════════════════════════════════════╣");
    Console.WriteLine("║  1. Tạo chiến dịch mẫu (nhanh 20-50 người nhận để test)           ║");
    Console.WriteLine("║  2. Tạo chiến dịch thủ công (nhập mới hoặc chọn người nhận)       ║");
    Console.WriteLine("║  3. Xem danh sách chiến dịch                                      ║");
    Console.WriteLine("║  4. Xem chi tiết chiến dịch                                       ║");
    Console.WriteLine("║  5. Kích hoạt chiến dịch (Start - Đẩy vào RabbitMQ)               ║");
    Console.WriteLine("║  6. Xem chi tiết kết quả gửi (Deliveries)                         ║");
    Console.WriteLine("║  7. Chạy lại các job thất bại (Retry Failed Jobs)                 ║");
    Console.WriteLine("║  8. Theo dõi tiến độ thời gian thực (Real-time Monitor)           ║");
    Console.WriteLine("║  9. Kiểm tra sức khỏe hệ thống (Health Check)                     ║");
    Console.WriteLine("║  0. Thoát                                                         ║");
    Console.WriteLine("╚═══════════════════════════════════════════════════════════════════╝");
    Console.ResetColor();
    Console.Write("\n👉 Nhập lựa chọn của bạn [0-9]: ");

    var choice = Console.ReadLine()?.Trim();
    if (choice is null) break;

    try
    {
        switch (choice)
        {
            case "1": await CreateSampleCampaign(); break;
            case "2": await CreateCustomCampaign(); break;
            case "3": await ListCampaigns(); break;
            case "4": await ViewCampaignDetail(); break;
            case "5": await StartCampaign(); break;
            case "6": await ViewDeliveries(); break;
            case "7": await RetryFailed(); break;
            case "8": await MonitorProgress(); break;
            case "9": await HealthCheck(); break;
            case "0": Console.WriteLine("Đang thoát chương trình. Tạm biệt!"); return;
            default:
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("⚠️  Lựa chọn không hợp lệ! Vui lòng chọn từ 0 đến 9.");
                Console.ResetColor();
                break;
        }
    }
    catch (HttpRequestException ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\n❌ Lỗi kết nối API: {ex.Message}");
        Console.WriteLine($"   Kiểm tra Campaign.Api đang chạy tại: {baseUrl}");
        Console.ResetColor();
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\n❌ Đã xảy ra lỗi: {ex.Message}");
        Console.ResetColor();
    }

    Console.WriteLine("\nNhấn Enter để quay lại menu...");
    try
    {
        if (!Console.IsInputRedirected)
        {
            Console.ReadKey(true);
        }
        else
        {
            Console.ReadLine();
        }
    }
    catch
    {
        Console.ReadLine();
    }
}

// ══════════════════════════════════════════════════════════
// 1. Tạo chiến dịch mẫu (20 - 50 người nhận)
// ══════════════════════════════════════════════════════════
async Task CreateSampleCampaign()
{
    Console.WriteLine("\n📧 TẠO CHIẾN DỊCH THÔNG BÁO MẪU (TESTING)");
    Console.WriteLine("───────────────────────────────────────────────────────────────────");
    Console.Write("Nhập số lượng người nhận mẫu (10-50, mặc định: 25): ");
    var countStr = Console.ReadLine()?.Trim();
    int count = int.TryParse(countStr, out int parsed) && parsed >= 5 && parsed <= 100 ? parsed : 25;

    Console.Write("Chọn kênh thông báo (1: Email, 2: SMS, 3: Push) [1]: ");
    var chChoice = Console.ReadLine()?.Trim();
    var channel = chChoice switch
    {
        "2" => "SMS",
        "3" => "Push",
        _ => "Email"
    };

    var firstNames = new[] { "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng", "Bùi", "Đỗ", "Hồ", "Ngô", "Dương" };
    var midNames = new[] { "Văn", "Thị", "Hữu", "Đức", "Minh", "Thanh", "Quốc", "Gia", "Bảo", "Anh" };
    var lastNames = new[] { "An", "Bình", "Cường", "Dũng", "Em", "Hùng", "Hải", "Khánh", "Long", "Nam", "Phong", "Quân", "Sơn", "Tuấn", "Vy" };

    var sampleRecipients = new List<object>();

    for (int i = 1; i <= count; i++)
    {
        var fullName = $"{firstNames[i % firstNames.Length]} {midNames[i % midNames.Length]} {lastNames[i % lastNames.Length]} #{i}";
        string destination;

        // Mô phỏng địa chỉ cố tình lỗi (~10% có từ khóa 'fail' để kiểm thử DLQ & Retry)
        if (i % 8 == 0)
        {
            destination = channel switch
            {
                "SMS" => $"+8490fail{i:D4}",
                "Push" => $"device_token_fail_{i:D4}",
                _ => $"fail_user_{i:D2}@notification-demo.com"
            };
        }
        else
        {
            destination = channel switch
            {
                "SMS" => $"+8490{Random.Shared.Next(1000000, 9999999)}",
                "Push" => $"fcm_token_{Guid.NewGuid().ToString("N")[..12]}",
                _ => $"user_{i:D2}@notification-demo.com"
            };
        }

        sampleRecipients.Add(new { Name = fullName, Destination = destination });
    }

    var request = new
    {
        Title = $"Chiến dịch {channel} Hàng loạt #{DateTime.Now:MMdd-HHmmss}",
        Content = $"[Khẩn] Thông báo quan trọng gửi qua kênh {channel} lúc {DateTime.Now:HH:mm:ss dd/MM/yyyy}.",
        Channel = channel,
        NewRecipients = sampleRecipients,
        ExistingRecipientIds = (List<Guid>?)null
    };

    Console.WriteLine($"\nĐang gửi yêu cầu tạo chiến dịch gồm {count} người nhận...");
    var response = await client.PostAsJsonAsync("/campaigns", request);
    response.EnsureSuccessStatusCode();
    var result = await response.Content.ReadFromJsonAsync<JsonElement>();

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"\n✅ TẠO CHIẾN DỊCH THÀNH CÔNG!");
    Console.WriteLine($"   Campaign ID:      {result.GetProperty("id")}");
    Console.WriteLine($"   Tiêu đề:          {result.GetProperty("title")}");
    Console.WriteLine($"   Tổng người nhận:  {result.GetProperty("recipientsCount")} (trạng thái: Pending)");
    Console.ResetColor();
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"\n💡 Gợi ý: Sao chép ID ở trên, chọn mục [5] để kích hoạt gửi và [8] để xem tiến độ!");
    Console.ResetColor();
}

// ══════════════════════════════════════════════════════════
// 2. Tạo chiến dịch thủ công
// ══════════════════════════════════════════════════════════
async Task CreateCustomCampaign()
{
    Console.WriteLine("\n📝 TẠO CHIẾN DỊCH MỚI (THỦ CÔNG)");
    Console.WriteLine("───────────────────────────────────────────────────────────────────");

    Console.Write("Tiêu đề chiến dịch: ");
    var title = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(title)) title = $"Chiến dịch #{DateTime.Now:yyyyMMdd-HHmm}";

    Console.Write("Nội dung thông báo: ");
    var content = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(content)) content = "Nội dung thông báo mặc định.";

    Console.Write("Kênh gửi (Email/SMS/Push) [Email]: ");
    var channel = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(channel)) channel = "Email";

    var recipients = new List<object>();
    var existingIds = new List<Guid>();

    // 2.1 Chọn người nhận có sẵn trong hệ thống (nếu có)
    Console.Write("\nBạn có muốn chọn người nhận từ danh sách có sẵn? (y/n) [n]: ");
    var wantExisting = Console.ReadLine()?.Trim().ToLowerInvariant();
    if (wantExisting == "y" || wantExisting == "yes")
    {
        var existingList = await client.GetFromJsonAsync<List<RecipientDtoResponse>>("/recipients", jsonOptions);
        if (existingList is { Count: > 0 })
        {
            Console.WriteLine($"\nDanh sách người nhận có sẵn ({existingList.Count} người):");
            for (int i = 0; i < Math.Min(existingList.Count, 15); i++)
            {
                var r = existingList[i];
                Console.WriteLine($"  [{i + 1}] {r.Name,-20} - {r.Destination} (ID: {r.Id})");
            }
            Console.Write("Nhập số thứ tự các người nhận muốn chọn (cách nhau bởi dấu phẩy, ví dụ: 1,3,5): ");
            var selectedIdx = Console.ReadLine()?.Trim();
            if (!string.IsNullOrEmpty(selectedIdx))
            {
                var indices = selectedIdx.Split(',').Select(s => s.Trim());
                foreach (var s in indices)
                {
                    if (int.TryParse(s, out int idx) && idx >= 1 && idx <= existingList.Count)
                    {
                        existingIds.Add(existingList[idx - 1].Id);
                    }
                }
                Console.WriteLine($"Đã chọn {existingIds.Count} người nhận có sẵn.");
            }
        }
        else
        {
            Console.WriteLine("Chưa có người nhận nào được lưu trước đó.");
        }
    }

    // 2.2 Nhập thêm người nhận mới
    Console.WriteLine("\nNhập danh sách người nhận MỚI (nhập Tên trống để kết thúc):");
    int index = 1;
    while (true)
    {
        Console.Write($"  [{index}] Tên người nhận: ");
        var name = Console.ReadLine()?.Trim();
        if (string.IsNullOrEmpty(name)) break;

        Console.Write($"  [{index}] Địa chỉ nhận ({channel}): ");
        var destination = Console.ReadLine()?.Trim() ?? "";

        recipients.Add(new { Name = name, Destination = destination });
        index++;
    }

    if (recipients.Count == 0 && existingIds.Count == 0)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("⚠️  Không có người nhận nào được thêm. Đã hủy tạo chiến dịch.");
        Console.ResetColor();
        return;
    }

    var request = new
    {
        Title = title,
        Content = content,
        Channel = channel,
        NewRecipients = recipients.Count > 0 ? recipients : null,
        ExistingRecipientIds = existingIds.Count > 0 ? existingIds : null
    };

    var response = await client.PostAsJsonAsync("/campaigns", request);
    response.EnsureSuccessStatusCode();
    var result = await response.Content.ReadFromJsonAsync<JsonElement>();

    Console.ForegroundColor = ConsoleColor.Green;
    Console.WriteLine($"\n✅ TẠO CHIẾN DỊCH THÀNH CÔNG!");
    Console.WriteLine($"   Campaign ID:      {result.GetProperty("id")}");
    Console.WriteLine($"   Tổng người nhận:  {result.GetProperty("recipientsCount")}");
    Console.ResetColor();
}

// ══════════════════════════════════════════════════════════
// 3. Xem danh sách chiến dịch
// ══════════════════════════════════════════════════════════
async Task ListCampaigns()
{
    Console.WriteLine("\n📋 DANH SÁCH CHIẾN DỊCH THÔNG BÁO");
    Console.WriteLine("───────────────────────────────────────────────────────────────────────────────────────────");

    var campaigns = await client.GetFromJsonAsync<List<CampaignResponse>>("/campaigns", jsonOptions);
    if (campaigns is null || campaigns.Count == 0)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("Chưa có chiến dịch nào trong hệ thống. Hãy chọn [1] hoặc [2] để tạo mới.");
        Console.ResetColor();
        return;
    }

    Console.WriteLine($"{"#",-3} {"ID",-36} {"Tiêu đề",-25} {"Kênh",-6} {"Trạng thái",-12} {"Pending",-8} {"Sent",-8} {"Failed",-8}");
    Console.WriteLine(new string('─', 110));

    int i = 1;
    foreach (var c in campaigns)
    {
        var statusColor = c.Status switch
        {
            "Draft" => ConsoleColor.DarkGray,
            "Processing" => ConsoleColor.Yellow,
            "Completed" => ConsoleColor.Green,
            _ => ConsoleColor.White
        };

        Console.Write($"{i,-3} {c.Id} {Truncate(c.Title, 23),-25} {c.Channel,-6} ");
        Console.ForegroundColor = statusColor;
        Console.Write($"{c.Status,-12}");
        Console.ResetColor();

        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.Write($"{c.PendingCount,-8}");
        Console.ForegroundColor = ConsoleColor.Green;
        Console.Write($"{c.SentCount,-8}");
        Console.ForegroundColor = ConsoleColor.Red;
        Console.Write($"{c.FailedCount,-8}");
        Console.ResetColor();
        Console.WriteLine();
        i++;
    }
}

// ══════════════════════════════════════════════════════════
// 4. Xem chi tiết chiến dịch
// ══════════════════════════════════════════════════════════
async Task ViewCampaignDetail()
{
    var id = PromptForCampaignId();
    if (id is null) return;

    var campaign = await client.GetFromJsonAsync<CampaignResponse>($"/campaigns/{id}", jsonOptions);
    if (campaign is null)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("❌ Không tìm thấy thông tin chiến dịch.");
        Console.ResetColor();
        return;
    }

    int total = campaign.PendingCount + campaign.SentCount + campaign.FailedCount;
    double successRate = total > 0 ? (double)campaign.SentCount / total * 100 : 0;

    Console.WriteLine($"\n╔══════════════════════ CHI TIẾT CHIẾN DỊCH ══════════════════════╗");
    Console.WriteLine($"  ID:          {campaign.Id}");
    Console.WriteLine($"  Tiêu đề:     {campaign.Title}");
    Console.WriteLine($"  Kênh gửi:    {campaign.Channel}");
    Console.WriteLine($"  Trạng thái:  {campaign.Status}");
    Console.WriteLine($"  Thời gian:   {campaign.CreatedAt:yyyy-MM-dd HH:mm:ss} UTC");
    Console.WriteLine($"  Nội dung:    {campaign.Content}");
    Console.WriteLine($"  ────────────────────────────────────────────────────────────────");
    Console.WriteLine($"  Tổng số job: {total}");
    Console.ForegroundColor = ConsoleColor.Cyan; Console.Write($"  Pending: {campaign.PendingCount,-6} ");
    Console.ForegroundColor = ConsoleColor.Green; Console.Write($"Sent: {campaign.SentCount,-6} ");
    Console.ForegroundColor = ConsoleColor.Red; Console.Write($"Failed: {campaign.FailedCount,-6} ");
    Console.ForegroundColor = ConsoleColor.White; Console.Write($"Tỷ lệ thành công: {successRate:F1}%\n");
    Console.ResetColor();
    Console.WriteLine($"╚══════════════════════════════════════════════════════════════════╝");
}

// ══════════════════════════════════════════════════════════
// 5. Kích hoạt chiến dịch (Start)
// ══════════════════════════════════════════════════════════
async Task StartCampaign()
{
    var id = PromptForCampaignId();
    if (id is null) return;

    Console.WriteLine($"\n🚀 Đang gửi yêu cầu kích hoạt chiến dịch {id}...");
    var response = await client.PostAsync($"/campaigns/{id}/start", null);

    if (response.IsSuccessStatusCode)
    {
        var result = await response.Content.ReadFromJsonAsync<JsonElement>();
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"\n✅ {result.GetProperty("message").GetString()}");
        Console.ResetColor();
        Console.WriteLine($"   Trạng thái HTTP: {(int)response.StatusCode} Accepted");
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("\n💡 Gợi ý: Chọn chức năng [8] để xem thanh tiến độ gửi theo thời gian thực!");
        Console.ResetColor();
    }
    else
    {
        var error = await response.Content.ReadAsStringAsync();
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\n❌ Kích hoạt thất bại (HTTP {(int)response.StatusCode}): {error}");
        Console.ResetColor();
    }
}

// ══════════════════════════════════════════════════════════
// 6. Xem kết quả gửi chi tiết (Deliveries)
// ══════════════════════════════════════════════════════════
async Task ViewDeliveries()
{
    var id = PromptForCampaignId();
    if (id is null) return;

    var deliveries = await client.GetFromJsonAsync<List<DeliveryResponse>>($"/campaigns/{id}/deliveries", jsonOptions);
    if (deliveries is null || deliveries.Count == 0)
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("Chiến dịch chưa có job gửi nào.");
        Console.ResetColor();
        return;
    }

    Console.WriteLine($"\n📬 KẾT QUẢ GỬI THÔNG BÁO ({deliveries.Count} jobs)");
    Console.WriteLine("──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────");
    Console.WriteLine($"{"#",-3} {"Người nhận",-18} {"Địa chỉ",-28} {"Trạng thái",-10} {"Worker",-12} {"Retry",-6} {"Correlation ID",-12} {"Lỗi (nếu có)",-25}");
    Console.WriteLine(new string('─', 125));

    int i = 1;
    foreach (var d in deliveries)
    {
        var statusColor = d.Status switch
        {
            "Pending" => ConsoleColor.Cyan,
            "Sent" => ConsoleColor.Green,
            "Failed" => ConsoleColor.Red,
            _ => ConsoleColor.White
        };

        Console.Write($"{i,-3} {Truncate(d.RecipientName, 16),-18} {Truncate(d.Destination, 26),-28} ");
        Console.ForegroundColor = statusColor;
        Console.Write($"{d.Status,-10}");
        Console.ResetColor();
        Console.Write($"{d.WorkerId ?? "-",-12} {d.RetryCount,-6} {d.CorrelationId[..8],-12} ");

        if (!string.IsNullOrEmpty(d.ErrorMessage))
        {
            Console.ForegroundColor = ConsoleColor.DarkYellow;
            Console.Write(Truncate(d.ErrorMessage, 25));
            Console.ResetColor();
        }
        Console.WriteLine();
        i++;
    }
}

// ══════════════════════════════════════════════════════════
// 7. Chạy lại job thất bại (Retry)
// ══════════════════════════════════════════════════════════
async Task RetryFailed()
{
    var id = PromptForCampaignId();
    if (id is null) return;

    Console.WriteLine($"\n🔄 Đang yêu cầu retry các job thất bại của chiến dịch {id}...");
    var response = await client.PostAsync($"/campaigns/{id}/retry", null);

    if (response.IsSuccessStatusCode)
    {
        var result = await response.Content.ReadFromJsonAsync<JsonElement>();
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"\n✅ {result.GetProperty("message").GetString()}");
        Console.ResetColor();
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine("💡 Gợi ý: Chọn [8] để tiếp tục theo dõi tiến độ gửi lại.");
        Console.ResetColor();
    }
    else
    {
        var error = await response.Content.ReadAsStringAsync();
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\n❌ Yêu cầu retry thất bại: {error}");
        Console.ResetColor();
    }
}

// ══════════════════════════════════════════════════════════
// 8. Theo dõi tiến độ thời gian thực (Real-time Monitor)
// ══════════════════════════════════════════════════════════
async Task MonitorProgress()
{
    var id = PromptForCampaignId();
    if (id is null) return;

    Console.WriteLine($"\n📊 Đang giám sát tiến độ chiến dịch {id}...");
    Console.WriteLine("   (Nhấn Ctrl+C để thoát khỏi chế độ giám sát)\n");

    var cts = new CancellationTokenSource();
    ConsoleCancelEventHandler? cancelHandler = null;

    try
    {
        cancelHandler = (s, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };
        Console.CancelKeyPress += cancelHandler;
    }
    catch { }

    try
    {
        while (!cts.Token.IsCancellationRequested)
        {
            var campaign = await client.GetFromJsonAsync<CampaignResponse>($"/campaigns/{id}", jsonOptions);
            if (campaign is null)
            {
                Console.WriteLine("Không tìm thấy chiến dịch.");
                break;
            }

            var total = campaign.PendingCount + campaign.SentCount + campaign.FailedCount;
            var processed = campaign.SentCount + campaign.FailedCount;
            var progress = total > 0 ? (double)processed / total * 100 : 0;
            var filled = Math.Min(40, (int)(progress / 2.5));
            var bar = new string('█', filled) + new string('░', 40 - filled);

            Console.Write($"\r  [{DateTime.Now:HH:mm:ss}] [{bar}] {progress,5:F1}%  ");
            Console.ForegroundColor = ConsoleColor.Cyan; Console.Write($"P:{campaign.PendingCount,-3} ");
            Console.ForegroundColor = ConsoleColor.Green; Console.Write($"S:{campaign.SentCount,-3} ");
            Console.ForegroundColor = ConsoleColor.Red; Console.Write($"F:{campaign.FailedCount,-3} ");
            Console.ResetColor();
            Console.Write($"| Trạng thái: {campaign.Status,-11} ");

            if (campaign.Status == "Completed" || (campaign.PendingCount == 0 && campaign.Status != "Draft"))
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine($"\n\n🎉 CHIẾN DỊCH ĐÃ HOÀN TẤT!");
                Console.WriteLine($"   Tổng job: {total} | Thành công: {campaign.SentCount} | Thất bại: {campaign.FailedCount}");
                Console.ResetColor();

                if (campaign.FailedCount > 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("   💡 Có job thất bại. Hãy chọn mục [7] trên menu chính để chạy lại!");
                    Console.ResetColor();
                }
                break;
            }

            await Task.Delay(1500, cts.Token);
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine("\n\nĐã dừng theo dõi tiến độ.");
    }
    finally
    {
        try
        {
            if (cancelHandler is not null)
            {
                Console.CancelKeyPress -= cancelHandler;
            }
        }
        catch { }
    }
}

// ══════════════════════════════════════════════════════════
// 9. Health Check
// ══════════════════════════════════════════════════════════
async Task HealthCheck()
{
    Console.WriteLine("\n🏥 KIỂM TRA SỨC KHỎE HỆ THỐNG (HEALTH CHECK)");
    Console.WriteLine("───────────────────────────────────────────────────────────────────");

    try
    {
        var response = await client.GetAsync("/health");
        var result = await response.Content.ReadFromJsonAsync<JsonElement>();

        var status = result.GetProperty("status").GetString();
        Console.Write("  Trạng thái tổng thể: ");
        Console.ForegroundColor = status == "Healthy" ? ConsoleColor.Green : ConsoleColor.Red;
        Console.WriteLine(status);
        Console.ResetColor();

        Console.WriteLine($"  PostgreSQL:          {result.GetProperty("database").GetString()}");
        Console.WriteLine($"  RabbitMQ:            {result.GetProperty("rabbitMQ").GetString()}");
        Console.WriteLine($"  Thời gian kiểm tra:  {result.GetProperty("timestamp").GetString()}");
    }
    catch (Exception ex)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"❌ Không thể kết nối tới Health Check API: {ex.Message}");
        Console.ResetColor();
    }
}

// ══════════════════════════════════════════════════════════
// Helper Functions & Models
// ══════════════════════════════════════════════════════════
string? PromptForCampaignId()
{
    Console.Write("\nNhập Campaign ID (Guid): ");
    var input = Console.ReadLine()?.Trim();
    if (string.IsNullOrEmpty(input) || !Guid.TryParse(input, out _))
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("⚠️  ID chiến dịch không hợp lệ (phải là định dạng Guid)!");
        Console.ResetColor();
        return null;
    }
    return input;
}

string Truncate(string? s, int maxLen)
{
    if (string.IsNullOrEmpty(s)) return "";
    return s.Length <= maxLen ? s : s[..(maxLen - 3)] + "...";
}

record CampaignResponse(
    Guid Id, string Title, string Content, string Channel,
    string Status, DateTime CreatedAt,
    int PendingCount, int SentCount, int FailedCount);

record DeliveryResponse(
    Guid Id, Guid CampaignId, Guid RecipientId,
    string RecipientName, string Destination,
    string Status, int RetryCount, string? ErrorMessage,
    string? WorkerId, string CorrelationId, DateTime UpdatedAt);

record RecipientDtoResponse(Guid Id, string Name, string Destination, DateTime CreatedAt);
