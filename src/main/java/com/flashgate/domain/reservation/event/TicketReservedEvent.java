package com.flashgate.domain.reservation.event;

import java.time.Instant;
import java.util.UUID;

public record TicketReservedEvent(
        UUID reservationId,
        UUID campaignId,
        UUID userId,
        String idempotencyKey,
        Instant expiresAt,
        Instant createdAt
) {
}
