using Microsoft.AspNetCore.SignalR;

namespace Campaign.Api.Hubs;

public class CampaignHub : Hub
{
    public async Task JoinCampaignGroup(string campaignId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, campaignId);
    }

    public async Task LeaveCampaignGroup(string campaignId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, campaignId);
    }
}
