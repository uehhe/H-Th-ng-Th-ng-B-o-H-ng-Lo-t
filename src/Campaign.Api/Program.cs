using Microsoft.EntityFrameworkCore;
using Campaign.Api.Services;
using Shared.Data;
using Shared.DTOs;
using Shared.Entities;
using Shared.Messages;
using CampaignEntity = Shared.Entities.Campaign;

var builder = WebApplication.CreateBuilder(args);

// ── Services ──────────────────────────────────────────────
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? "Host=postgres;Database=notifications;Username=postgres;Password=postgres";

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

builder.Services.AddSingleton<RabbitMqPublisher>();

var app = builder.Build();

// ── Database init with retry ──────────────────────────────
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
    for (int i = 0; i < 15; i++)
    {
        try
        {
            await db.Database.EnsureCreatedAsync();
            logger.LogInformation("Database initialized successfully.");
            break;
        }
        catch (Exception ex)
        {
            try
            {
                if (await db.Database.CanConnectAsync())
                {
                    logger.LogInformation("Database connected successfully (initialized by another instance).");
                    break;
                }
            }
            catch { }

            logger.LogWarning("DB not ready, retry {Retry}/15... {Error}", i + 1, ex.Message);
            await Task.Delay(3000);
        }
    }
}

// ── RabbitMQ init with graceful fallback ─────────────────
var publisher = app.Services.GetRequiredService<RabbitMqPublisher>();
try
{
    await publisher.InitializeAsync();
}
catch (Exception ex)
{
    app.Logger.LogWarning("RabbitMQ initial connection deferred: {Message}", ex.Message);
}

// ── API Endpoints ─────────────────────────────────────────

// 1. POST /campaigns - Create a new campaign with recipients
app.MapPost("/campaigns", async (CreateCampaignRequest request, AppDbContext db) =>
{
    var campaign = new CampaignEntity
    {
        Title = request.Title,
        Content = request.Content,
        Channel = request.Channel,
        Status = CampaignStatus.Draft,
        CreatedAt = DateTime.UtcNow
    };
    db.Campaigns.Add(campaign);

    var recipientIds = new List<Guid>();

    // Add new recipients
    if (request.NewRecipients is { Count: > 0 })
    {
        foreach (var r in request.NewRecipients)
        {
            var recipient = new Recipient
            {
                Name = r.Name,
                Destination = r.Destination,
                CreatedAt = DateTime.UtcNow
            };
            db.Recipients.Add(recipient);
            recipientIds.Add(recipient.Id);
        }
    }

    // Add existing recipients
    if (request.ExistingRecipientIds is { Count: > 0 })
    {
        var existingIds = await db.Recipients
            .Where(r => request.ExistingRecipientIds.Contains(r.Id))
            .Select(r => r.Id)
            .ToListAsync();
        recipientIds.AddRange(existingIds);
    }

    // Create Delivery records (all Pending)
    foreach (var recipientId in recipientIds)
    {
        db.Deliveries.Add(new Delivery
        {
            CampaignId = campaign.Id,
            RecipientId = recipientId,
            Status = "Pending",
            CorrelationId = Guid.NewGuid().ToString(),
            UpdatedAt = DateTime.UtcNow
        });
    }

    await db.SaveChangesAsync();
    return Results.Created($"/campaigns/{campaign.Id}", new { campaign.Id, campaign.Title, RecipientsCount = recipientIds.Count });
});

// 2. GET /campaigns - List all campaigns with delivery stats
app.MapGet("/campaigns", async (AppDbContext db) =>
{
    var campaigns = await db.Campaigns
        .OrderByDescending(c => c.CreatedAt)
        .Select(c => new CampaignDto(
            c.Id, c.Title, c.Content, c.Channel,
            c.Status.ToString(), c.CreatedAt,
            c.Deliveries.Count(d => d.Status == "Pending"),
            c.Deliveries.Count(d => d.Status == "Sent"),
            c.Deliveries.Count(d => d.Status == "Failed")
        ))
        .ToListAsync();

    return Results.Ok(campaigns);
});

// 3. GET /campaigns/{id} - Get campaign detail with real-time stats
app.MapGet("/campaigns/{id:guid}", async (Guid id, AppDbContext db) =>
{
    var campaign = await db.Campaigns
        .Where(c => c.Id == id)
        .Select(c => new CampaignDto(
            c.Id, c.Title, c.Content, c.Channel,
            c.Status.ToString(), c.CreatedAt,
            c.Deliveries.Count(d => d.Status == "Pending"),
            c.Deliveries.Count(d => d.Status == "Sent"),
            c.Deliveries.Count(d => d.Status == "Failed")
        ))
        .FirstOrDefaultAsync();

    return campaign is null ? Results.NotFound() : Results.Ok(campaign);
});

// 4. POST /campaigns/{id}/start - Start campaign, publish pending jobs to RabbitMQ
app.MapPost("/campaigns/{id:guid}/start", async (Guid id, AppDbContext db, RabbitMqPublisher rabbitPublisher) =>
{
    var campaign = await db.Campaigns.FindAsync(id);
    if (campaign is null) return Results.NotFound();
    if (campaign.Status == CampaignStatus.Completed)
        return Results.BadRequest(new { Error = "Campaign is already completed." });

    campaign.Status = CampaignStatus.Processing;

    var pendingDeliveries = await db.Deliveries
        .Where(d => d.CampaignId == id && d.Status == "Pending")
        .Include(d => d.Recipient)
        .ToListAsync();

    if (pendingDeliveries.Count == 0)
        return Results.BadRequest(new { Error = "No pending deliveries to process." });

    await db.SaveChangesAsync();

    // Build messages from loaded data (before scope disposal)
    var messages = pendingDeliveries.Select(d => new NotificationJobMessage(
        d.Id, campaign.Id, d.RecipientId,
        d.Recipient.Destination, campaign.Channel,
        campaign.Content, d.CorrelationId
    )).ToList();

    // Fire-and-forget: publish to RabbitMQ asynchronously, return 202 immediately
    _ = Task.Run(async () =>
    {
        foreach (var msg in messages)
        {
            try
            {
                await rabbitPublisher.PublishAsync(msg);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Failed to publish delivery {msg.DeliveryId}: {ex.Message}");
            }
        }
    });

    return Results.Accepted(value: new { Message = $"Started processing {pendingDeliveries.Count} deliveries.", CampaignId = id });
});

// 5. GET /campaigns/{id}/deliveries - Query delivery results
app.MapGet("/campaigns/{id:guid}/deliveries", async (Guid id, AppDbContext db) =>
{
    var deliveries = await db.Deliveries
        .Where(d => d.CampaignId == id)
        .Include(d => d.Recipient)
        .OrderBy(d => d.Recipient.Name)
        .Select(d => new DeliveryDto(
            d.Id, d.CampaignId, d.RecipientId,
            d.Recipient.Name, d.Recipient.Destination,
            d.Status, d.RetryCount, d.ErrorMessage,
            d.WorkerId, d.CorrelationId, d.UpdatedAt
        ))
        .ToListAsync();

    return Results.Ok(deliveries);
});

// 6. POST /campaigns/{id}/retry - Retry failed deliveries
app.MapPost("/campaigns/{id:guid}/retry", async (Guid id, AppDbContext db, RabbitMqPublisher rabbitPublisher) =>
{
    var campaign = await db.Campaigns.FindAsync(id);
    if (campaign is null) return Results.NotFound();

    var failedDeliveries = await db.Deliveries
        .Where(d => d.CampaignId == id && d.Status == "Failed")
        .Include(d => d.Recipient)
        .ToListAsync();

    if (failedDeliveries.Count == 0)
        return Results.Ok(new { Message = "No failed deliveries to retry." });

    foreach (var delivery in failedDeliveries)
    {
        delivery.Status = "Pending";
        delivery.RetryCount++;
        delivery.ErrorMessage = null;
        delivery.UpdatedAt = DateTime.UtcNow;
    }

    campaign.Status = CampaignStatus.Processing;
    await db.SaveChangesAsync();

    var messages = failedDeliveries.Select(d => new NotificationJobMessage(
        d.Id, campaign.Id, d.RecipientId,
        d.Recipient.Destination, campaign.Channel,
        campaign.Content, d.CorrelationId
    )).ToList();

    _ = Task.Run(async () =>
    {
        foreach (var msg in messages)
        {
            try { await rabbitPublisher.PublishAsync(msg); }
            catch (Exception ex) { Console.Error.WriteLine($"Failed to publish retry {msg.DeliveryId}: {ex.Message}"); }
        }
    });

    return Results.Accepted(value: new { Message = $"Retrying {failedDeliveries.Count} failed deliveries.", CampaignId = id });
});

// 7. GET /health - Health check endpoint
app.MapGet("/health", async (AppDbContext db, RabbitMqPublisher rabbitPublisher) =>
{
    bool dbHealthy = false;
    try { dbHealthy = await db.Database.CanConnectAsync(); } catch { }
    bool rabbitHealthy = rabbitPublisher.IsConnected;

    var result = new
    {
        Status = dbHealthy && rabbitHealthy ? "Healthy" : "Unhealthy",
        Database = dbHealthy ? "Connected" : "Disconnected",
        RabbitMQ = rabbitHealthy ? "Connected" : "Disconnected",
        Timestamp = DateTime.UtcNow
    };

    return dbHealthy && rabbitHealthy
        ? Results.Ok(result)
        : Results.Json(result, statusCode: 503);
});

// Bonus: GET /recipients - List all recipients
app.MapGet("/recipients", async (AppDbContext db) =>
{
    var recipients = await db.Recipients
        .OrderBy(r => r.Name)
        .Select(r => new RecipientDto(r.Id, r.Name, r.Destination, r.CreatedAt))
        .ToListAsync();
    return Results.Ok(recipients);
});

await app.RunAsync();
