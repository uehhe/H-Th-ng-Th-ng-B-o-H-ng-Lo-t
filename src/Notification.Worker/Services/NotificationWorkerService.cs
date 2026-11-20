using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shared.Data;
using Shared.Entities;
using Shared.Messages;

namespace Notification.Worker.Services;

public class NotificationWorkerService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _config;
    private readonly ILogger<NotificationWorkerService> _logger;
    private readonly string _workerId;

    public NotificationWorkerService(
        IServiceScopeFactory scopeFactory,
        IConfiguration config,
        ILogger<NotificationWorkerService> logger)
    {
        _scopeFactory = scopeFactory;
        _config = config;
        _logger = logger;
        _workerId = Environment.GetEnvironmentVariable("WORKER_ID")
            ?? Environment.MachineName;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("[{WorkerId}] Notification Worker starting...", _workerId);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConnectAndConsumeAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[{WorkerId}] Connection lost or error occurred. Reconnecting in 5 seconds...", _workerId);
                try { await Task.Delay(5000, stoppingToken); } catch { break; }
            }
        }

        _logger.LogInformation("[{WorkerId}] Notification Worker stopped.", _workerId);
    }

    private async Task ConnectAndConsumeAsync(CancellationToken stoppingToken)
    {
        var hostName = _config["RabbitMQ:HostName"] ?? "rabbitmq";
        var userName = _config["RabbitMQ:UserName"] ?? "guest";
        var password = _config["RabbitMQ:Password"] ?? "guest";

        var factory = new ConnectionFactory
        {
            HostName = hostName,
            UserName = userName,
            Password = password
        };

        _logger.LogInformation("[{WorkerId}] Connecting to RabbitMQ at {Host}...", _workerId, hostName);

        await using var connection = await factory.CreateConnectionAsync(stoppingToken);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        // Declare Dead-Letter Queue (DLQ)
        await channel.QueueDeclareAsync(
            queue: "notifications.dlq",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            noWait: false,
            cancellationToken: stoppingToken);

        // Declare main queue with DLQ routing
        var queueArgs = new Dictionary<string, object?>
        {
            ["x-dead-letter-exchange"] = "",
            ["x-dead-letter-routing-key"] = "notifications.dlq"
        };

        await channel.QueueDeclareAsync(
            queue: "notification-jobs",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueArgs,
            noWait: false,
            cancellationToken: stoppingToken);

        // Fair dispatch: 1 message at a time per worker instance
        await channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken);

        _logger.LogInformation("[{WorkerId}] Connected to RabbitMQ. Waiting for notification jobs...", _workerId);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (sender, ea) =>
        {
            await ProcessMessageAsync(channel, ea);
        };

        await channel.BasicConsumeAsync(
            queue: "notification-jobs",
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        // Keep running until disconnected or cancellation requested
        var tcs = new TaskCompletionSource();

        connection.ConnectionShutdownAsync += (sender, ea) =>
        {
            tcs.TrySetException(new InvalidOperationException($"RabbitMQ connection shutdown: {ea.ReplyText}"));
            return Task.CompletedTask;
        };

        using var registration = stoppingToken.Register(() => tcs.TrySetCanceled());
        await tcs.Task;
    }

    private async Task ProcessMessageAsync(IChannel channel, BasicDeliverEventArgs ea)
    {
        NotificationJobMessage? message = null;

        try
        {
            var body = ea.Body.ToArray();
            message = JsonSerializer.Deserialize<NotificationJobMessage>(body);

            if (message is null)
            {
                _logger.LogWarning("[{WorkerId}] Received invalid/null message. Rejecting to DLQ.", _workerId);
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var delivery = await db.Deliveries.FindAsync(message.DeliveryId);
            if (delivery is null)
            {
                _logger.LogWarning("[{WorkerId}] [{CorrelationId}] Delivery {DeliveryId} not found in DB. Acking.",
                    _workerId, message.CorrelationId, message.DeliveryId);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                return;
            }

            // ── Idempotent Consumer: skip if already Sent ──
            if (delivery.Status == "Sent")
            {
                _logger.LogInformation(
                    "[{WorkerId}] [{CorrelationId}] Delivery {DeliveryId} already Sent. Skipping duplicate processing (Idempotent).",
                    _workerId, message.CorrelationId, message.DeliveryId);
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
                return;
            }

            _logger.LogInformation(
                "[{WorkerId}] [{CorrelationId}] Processing Delivery {DeliveryId} | Campaign {CampaignId} -> {Destination} ({Channel})",
                _workerId, message.CorrelationId, message.DeliveryId, message.CampaignId, message.Destination, message.Channel);

            // ── Simulate sending network delay (300–500ms) ──
            var delay = Random.Shared.Next(300, 501);
            await Task.Delay(delay);

            // ── Simulate failure conditions for testing ──
            bool shouldFail = false;
            string? errorMessage = null;

            if (message.Destination.Contains("fail", StringComparison.OrdinalIgnoreCase))
            {
                shouldFail = true;
                errorMessage = $"Simulated failure: destination '{message.Destination}' contains 'fail' keyword.";
            }
            else if (delivery.RetryCount == 0 && Random.Shared.NextDouble() < 0.15)
            {
                shouldFail = true;
                errorMessage = "Simulated random network timeout (~15% on first attempt).";
            }

            if (shouldFail)
            {
                delivery.Status = "Failed";
                delivery.ErrorMessage = errorMessage;
                delivery.WorkerId = _workerId;
                delivery.UpdatedAt = DateTime.UtcNow;
                await db.SaveChangesAsync();

                _logger.LogWarning(
                    "[{WorkerId}] [{CorrelationId}] Delivery {DeliveryId} ❌ FAILED: {Error}",
                    _workerId, message.CorrelationId, message.DeliveryId, errorMessage);

                // Ack ONLY AFTER DB update (Fault tolerance guarantee)
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }
            else
            {
                delivery.Status = "Sent";
                delivery.WorkerId = _workerId;
                delivery.ErrorMessage = null;
                delivery.UpdatedAt = DateTime.UtcNow;
                await db.SaveChangesAsync();

                _logger.LogInformation(
                    "[{WorkerId}] [{CorrelationId}] Delivery {DeliveryId} ✅ SENT to {Destination}",
                    _workerId, message.CorrelationId, message.DeliveryId, message.Destination);

                // Ack ONLY AFTER DB update (Fault tolerance guarantee)
                await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
            }

            // ── Check if campaign has finished all jobs ──
            try
            {
                var pendingCount = await db.Deliveries
                    .CountAsync(d => d.CampaignId == message.CampaignId && d.Status == "Pending");

                if (pendingCount == 0)
                {
                    var campaign = await db.Campaigns.FindAsync(message.CampaignId);
                    if (campaign is not null && campaign.Status == CampaignStatus.Processing)
                    {
                        campaign.Status = CampaignStatus.Completed;
                        await db.SaveChangesAsync();
                        _logger.LogInformation("[{WorkerId}] Campaign {CampaignId} marked as ✅ Completed.",
                            _workerId, message.CampaignId);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("[{WorkerId}] Campaign status check handled: {Message}", _workerId, ex.Message);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "[{WorkerId}] Unhandled error processing message. Rejecting to DLQ. DeliveryId={DeliveryId}",
                _workerId, message?.DeliveryId);

            try
            {
                await channel.BasicNackAsync(ea.DeliveryTag, multiple: false, requeue: false);
            }
            catch (Exception nackEx)
            {
                _logger.LogError(nackEx, "[{WorkerId}] Failed to Nack message.", _workerId);
            }
        }
    }
}
