namespace Shared.Entities;

public class Recipient
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = string.Empty;
    public string Destination { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public ICollection<Delivery> Deliveries { get; set; } = new List<Delivery>();
}
