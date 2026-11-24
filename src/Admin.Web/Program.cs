using Admin.Web.Components;
using Admin.Web.Services;

var builder = WebApplication.CreateBuilder(args);

// Add Razor components with Interactive Server mode
builder.Services.AddRazorComponents()
    .AddInteractiveServerComponents();

// Register CampaignApiClient for REST calls
var apiUrl = builder.Configuration["CampaignApiUrl"]
    ?? Environment.GetEnvironmentVariable("CAMPAIGN_API_URL")
    ?? "http://localhost:5000";

builder.Services.AddHttpClient<CampaignApiClient>(client =>
{
    client.BaseAddress = new Uri(apiUrl.TrimEnd('/') + "/");
    client.Timeout = TimeSpan.FromSeconds(30);
});

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseAntiforgery();
app.MapStaticAssets();

app.MapRazorComponents<App>()
    .AddInteractiveServerRenderMode();

await app.RunAsync();
