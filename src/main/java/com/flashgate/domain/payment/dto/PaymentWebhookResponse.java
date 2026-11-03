package com.flashgate.domain.payment.dto;

import java.util.UUID;

public record PaymentWebhookResponse(
        UUID reservationId,
        String status,
        String message
) {
}
