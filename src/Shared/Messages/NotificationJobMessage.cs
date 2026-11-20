namespace Shared.Messages;

public record NotificationJobMessage(
    Guid DeliveryId,
    Guid CampaignId,
    Guid RecipientId,
    string Destination,
    string Channel,
    string Content,
    string CorrelationId
);
