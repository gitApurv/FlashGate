package com.flashgate.domain.campaign.dto;

import java.time.Instant;
import java.util.UUID;

public record CampaignCacheDto(
        UUID campaignId,
        Instant saleStartsAt,
        Instant saleEndsAt
) {
}
