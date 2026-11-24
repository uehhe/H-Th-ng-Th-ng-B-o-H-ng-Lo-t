using System.Net.Http.Json;
using System.Text.Json;
using Shared.DTOs;

namespace Admin.Web.Services;

public record HealthCheckResponse(
    string Status,
    string Database,
    string RabbitMQ,
    DateTime Timestamp
);

public record CreateCampaignResult(Guid Id, string? Title, int RecipientsCount);

public class CampaignApiClient
{
    private readonly HttpClient _http;
    private readonly JsonSerializerOptions _jsonOptions;

    public CampaignApiClient(HttpClient http)
    {
        _http = http;
        _jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    }

    public async Task<List<CampaignDto>> GetCampaignsAsync()
    {
        var result = await _http.GetFromJsonAsync<List<CampaignDto>>("campaigns", _jsonOptions);
        return result ?? new List<CampaignDto>();
    }

    public async Task<CampaignDto?> GetCampaignAsync(Guid id)
    {
        try
        {
            return await _http.GetFromJsonAsync<CampaignDto>($"campaigns/{id}", _jsonOptions);
        }
        catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<List<DeliveryDto>> GetDeliveriesAsync(Guid campaignId)
    {
        var result = await _http.GetFromJsonAsync<List<DeliveryDto>>($"campaigns/{campaignId}/deliveries", _jsonOptions);
        return result ?? new List<DeliveryDto>();
    }

    public async Task<CreateCampaignResult?> CreateCampaignAsync(CreateCampaignRequest request)
    {
        var response = await _http.PostAsJsonAsync("campaigns", request);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<CreateCampaignResult>(_jsonOptions);
    }

    public async Task<bool> StartCampaignAsync(Guid campaignId)
    {
        var response = await _http.PostAsync($"campaigns/{campaignId}/start", null);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> RetryCampaignAsync(Guid campaignId)
    {
        var response = await _http.PostAsync($"campaigns/{campaignId}/retry", null);
        return response.IsSuccessStatusCode;
    }

    public async Task<HealthCheckResponse?> GetHealthAsync()
    {
        try
        {
            return await _http.GetFromJsonAsync<HealthCheckResponse>("health", _jsonOptions);
        }
        catch
        {
            return new HealthCheckResponse("Unhealthy", "Disconnected", "Disconnected", DateTime.UtcNow);
        }
    }

    public async Task<List<RecipientDto>> GetRecipientsAsync()
    {
        try
        {
            var result = await _http.GetFromJsonAsync<List<RecipientDto>>("recipients", _jsonOptions);
            return result ?? new List<RecipientDto>();
        }
        catch
        {
            return new List<RecipientDto>();
        }
    }
}
