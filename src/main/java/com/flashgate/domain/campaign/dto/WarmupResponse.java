package com.flashgate.domain.campaign.dto;

import java.time.Instant;
import java.util.UUID;

public record WarmupResponse(
        UUID campaignId,
        Integer warmedStock,
        Instant saleStartsAt,
        Instant saleEndsAt,
        String status
) {
}
