namespace Shared.DTOs;

public record RecipientInput(string Name, string Destination);

public record CreateCampaignRequest(
    string Title,
    string Content,
    string Channel,
    List<RecipientInput>? NewRecipients,
    List<Guid>? ExistingRecipientIds
);

public record CampaignDto(
    Guid Id,
    string Title,
    string Content,
    string Channel,
    string Status,
    DateTime CreatedAt,
    int PendingCount,
    int SentCount,
    int FailedCount
);

public record DeliveryDto(
    Guid Id,
    Guid CampaignId,
    Guid RecipientId,
    string RecipientName,
    string Destination,
    string Status,
    int RetryCount,
    string? ErrorMessage,
    string? WorkerId,
    string CorrelationId,
    DateTime UpdatedAt
);

public record RecipientDto(Guid Id, string Name, string Destination, DateTime CreatedAt);
