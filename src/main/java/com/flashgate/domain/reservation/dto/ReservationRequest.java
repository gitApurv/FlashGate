package com.flashgate.domain.reservation.dto;

import java.util.UUID;

public record ReservationRequest(
        UUID campaignId,
        UUID userId,
        String idempotencyKey,
        ConcurrencyMode concurrencyMode
) {
    public ReservationRequest {
        if (concurrencyMode == null) {
            concurrencyMode = ConcurrencyMode.REDIS_KAFKA_PIPELINE;
        }
    }
}
