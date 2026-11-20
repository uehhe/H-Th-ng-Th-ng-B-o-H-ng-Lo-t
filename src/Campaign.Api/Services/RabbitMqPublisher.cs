using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using Shared.Messages;

namespace Campaign.Api.Services;

public class RabbitMqPublisher : IAsyncDisposable
{
    private IConnection? _connection;
    private IChannel? _channel;
    private readonly IConfiguration _config;
    private readonly ILogger<RabbitMqPublisher> _logger;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public bool IsConnected => _connection is { IsOpen: true } && _channel is { IsOpen: true };

    public RabbitMqPublisher(IConfiguration config, ILogger<RabbitMqPublisher> logger)
    {
        _config = config;
        _logger = logger;
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureConnectedInternalAsync(ct);
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task EnsureConnectedInternalAsync(CancellationToken ct = default)
    {
        if (IsConnected) return;

        var hostName = _config["RabbitMQ:HostName"] ?? "rabbitmq";
        var userName = _config["RabbitMQ:UserName"] ?? "guest";
        var password = _config["RabbitMQ:Password"] ?? "guest";

        for (int i = 0; i < 15; i++)
        {
            try
            {
                var factory = new ConnectionFactory
                {
                    HostName = hostName,
                    UserName = userName,
                    Password = password
                };

                _connection = await factory.CreateConnectionAsync(ct);
                _channel = await _connection.CreateChannelAsync(cancellationToken: ct);

                // Declare Dead-Letter Queue (DLQ)
                await _channel.QueueDeclareAsync(
                    queue: "notifications.dlq",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null,
                    noWait: false,
                    cancellationToken: ct);

                // Declare main queue with DLQ routing
                var args = new Dictionary<string, object?>
                {
                    ["x-dead-letter-exchange"] = "",
                    ["x-dead-letter-routing-key"] = "notifications.dlq"
                };

                await _channel.QueueDeclareAsync(
                    queue: "notification-jobs",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: args,
                    noWait: false,
                    cancellationToken: ct);

                _logger.LogInformation("Connected to RabbitMQ at {Host}", hostName);
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning("RabbitMQ not ready, retry {Retry}/15... {Error}", i + 1, ex.Message);
                await Task.Delay(3000, ct);
            }
        }

        throw new InvalidOperationException("Failed to connect to RabbitMQ after 15 retries");
    }

    public async Task PublishAsync(NotificationJobMessage message, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            if (!IsConnected)
            {
                await EnsureConnectedInternalAsync(ct);
            }

            if (_channel is null) throw new InvalidOperationException("RabbitMQ channel unavailable");

            var body = JsonSerializer.SerializeToUtf8Bytes(message);

            var properties = new BasicProperties
            {
                Persistent = true,
                ContentType = "application/json",
                CorrelationId = message.CorrelationId
            };

            await _channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: "notification-jobs",
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: ct);

            _logger.LogDebug("Published job for Delivery {DeliveryId}, CorrelationId={CorrelationId}",
                message.DeliveryId, message.CorrelationId);
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync();
            _channel.Dispose();
        }
        if (_connection is not null)
        {
            await _connection.CloseAsync();
            _connection.Dispose();
        }
        _lock.Dispose();
    }
}
