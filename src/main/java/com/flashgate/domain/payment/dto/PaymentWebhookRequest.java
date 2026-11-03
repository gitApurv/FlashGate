package com.flashgate.domain.payment.dto;

import java.util.UUID;

public record PaymentWebhookRequest(
        UUID reservationId,
        String paymentId,
        String eventType,
        Long amountCents
) {
}
