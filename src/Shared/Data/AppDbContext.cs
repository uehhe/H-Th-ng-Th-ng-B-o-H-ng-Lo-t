using Microsoft.EntityFrameworkCore;
using Shared.Entities;

namespace Shared.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<Campaign> Campaigns => Set<Campaign>();
    public DbSet<Recipient> Recipients => Set<Recipient>();
    public DbSet<Delivery> Deliveries => Set<Delivery>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Campaign>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Status)
                  .HasConversion<string>()
                  .HasDefaultValue(CampaignStatus.Draft);
            entity.Property(e => e.CreatedAt).HasDefaultValueSql("NOW()");
        });

        modelBuilder.Entity<Recipient>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.CreatedAt).HasDefaultValueSql("NOW()");
        });

        modelBuilder.Entity<Delivery>(entity =>
        {
            entity.HasKey(e => e.Id);
            entity.Property(e => e.Status).HasDefaultValue("Pending");
            entity.Property(e => e.RetryCount).HasDefaultValue(0);
            entity.Property(e => e.UpdatedAt).HasDefaultValueSql("NOW()");

            entity.HasOne(e => e.Campaign)
                  .WithMany(c => c.Deliveries)
                  .HasForeignKey(e => e.CampaignId)
                  .OnDelete(DeleteBehavior.Cascade);

            entity.HasOne(e => e.Recipient)
                  .WithMany(r => r.Deliveries)
                  .HasForeignKey(e => e.RecipientId)
                  .OnDelete(DeleteBehavior.Cascade);

            entity.HasIndex(e => e.CampaignId);
            entity.HasIndex(e => e.RecipientId);
            entity.HasIndex(e => e.CorrelationId);
        });
    }
}
