using Microsoft.EntityFrameworkCore;
using Shared.Data;
using Notification.Worker.Services;

var builder = Host.CreateApplicationBuilder(args);

// ── Database ──
var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? "Host=postgres;Database=notifications;Username=postgres;Password=postgres";

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

// ── Worker Service ──
builder.Services.AddHostedService<NotificationWorkerService>();

var host = builder.Build();

// ── Database init with retry ──
var logger = host.Services.GetRequiredService<ILogger<Program>>();
var workerId = Environment.GetEnvironmentVariable("WORKER_ID") ?? Environment.MachineName;

using (var scope = host.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    for (int i = 0; i < 15; i++)
    {
        try
        {
            await db.Database.EnsureCreatedAsync();
            logger.LogInformation("[{WorkerId}] Database initialized successfully.", workerId);
            break;
        }
        catch (Exception ex)
        {
            try
            {
                if (await db.Database.CanConnectAsync())
                {
                    logger.LogInformation("[{WorkerId}] Database connected successfully (initialized by another instance).", workerId);
                    break;
                }
            }
            catch { }

            logger.LogWarning("[{WorkerId}] DB not ready, retry {Retry}/15... {Error}", workerId, i + 1, ex.Message);
            await Task.Delay(3000);
        }
    }
}

await host.RunAsync();
