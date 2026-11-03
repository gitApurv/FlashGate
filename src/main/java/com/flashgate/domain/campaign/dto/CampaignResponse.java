package com.flashgate.domain.campaign.dto;

import java.time.Instant;
import java.util.UUID;

public record CampaignResponse(
        UUID id,
        String title,
        Integer totalStock,
        Integer availableStock,
        Long priceCents,
        Instant saleStartsAt,
        Instant saleEndsAt,
        Instant createdAt
) {
}
