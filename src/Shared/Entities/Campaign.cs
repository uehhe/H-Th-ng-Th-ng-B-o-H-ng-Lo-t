namespace Shared.Entities;

public enum CampaignStatus
{
    Draft,
    Processing,
    Completed
}

public class Campaign
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Title { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Channel { get; set; } = "Email";
    public CampaignStatus Status { get; set; } = CampaignStatus.Draft;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public ICollection<Delivery> Deliveries { get; set; } = new List<Delivery>();
}
