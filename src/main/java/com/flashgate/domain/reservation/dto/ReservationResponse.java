package com.flashgate.domain.reservation.dto;

import java.time.Instant;
import java.util.UUID;

public record ReservationResponse(
        UUID reservationId,
        UUID campaignId,
        UUID userId,
        String status,
        Instant expiresAt,
        String statusCheckUrl
) {
    public static ReservationResponse from(UUID reservationId, UUID campaignId, UUID userId, String status, Instant expiresAt) {
        return new ReservationResponse(
                reservationId,
                campaignId,
                userId,
                status,
                expiresAt,
                "/api/v1/reservations/" + reservationId
        );
    }
}
