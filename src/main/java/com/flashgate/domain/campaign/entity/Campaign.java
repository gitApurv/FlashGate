package com.flashgate.domain.campaign.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "campaigns")
public class Campaign {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "title", nullable = false)
    private String title;

    @Column(name = "total_stock", nullable = false)
    private Integer totalStock;

    @Column(name = "available_stock", nullable = false)
    private Integer availableStock;

    @Column(name = "price_cents", nullable = false)
    private Long priceCents;

    @Version
    @Column(name = "version", nullable = false)
    private Long version = 0L;

    @Column(name = "sale_starts_at", nullable = false)
    private Instant saleStartsAt;

    @Column(name = "sale_ends_at", nullable = false)
    private Instant saleEndsAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();

    public Campaign() {
    }

    public Campaign(UUID id, String title, Integer totalStock, Integer availableStock, Long priceCents,
                    Instant saleStartsAt, Instant saleEndsAt) {
        this.id = id;
        this.title = title;
        this.totalStock = totalStock;
        this.availableStock = availableStock;
        this.priceCents = priceCents;
        this.saleStartsAt = saleStartsAt;
        this.saleEndsAt = saleEndsAt;
        this.createdAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getTotalStock() {
        return totalStock;
    }

    public void setTotalStock(Integer totalStock) {
        this.totalStock = totalStock;
    }

    public Integer getAvailableStock() {
        return availableStock;
    }

    public void setAvailableStock(Integer availableStock) {
        this.availableStock = availableStock;
    }

    public Long getPriceCents() {
        return priceCents;
    }

    public void setPriceCents(Long priceCents) {
        this.priceCents = priceCents;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Instant getSaleStartsAt() {
        return saleStartsAt;
    }

    public void setSaleStartsAt(Instant saleStartsAt) {
        this.saleStartsAt = saleStartsAt;
    }

    public Instant getSaleEndsAt() {
        return saleEndsAt;
    }

    public void setSaleEndsAt(Instant saleEndsAt) {
        this.saleEndsAt = saleEndsAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
}
