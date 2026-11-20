namespace Shared.Entities;

public class Delivery
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid CampaignId { get; set; }
    public Guid RecipientId { get; set; }
    public string Status { get; set; } = "Pending";
    public int RetryCount { get; set; } = 0;
    public string? ErrorMessage { get; set; }
    public string? WorkerId { get; set; }
    public string CorrelationId { get; set; } = Guid.NewGuid().ToString();
    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    public Campaign Campaign { get; set; } = null!;
    public Recipient Recipient { get; set; } = null!;
}
