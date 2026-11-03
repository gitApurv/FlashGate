package com.flashgate.domain.payment.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flashgate.domain.payment.dto.PaymentWebhookRequest;
import com.flashgate.domain.payment.dto.PaymentWebhookResponse;
import com.flashgate.domain.payment.service.PaymentWebhookService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/webhooks")
public class PaymentWebhookController {

    private final PaymentWebhookService paymentWebhookService;
    private final ObjectMapper objectMapper;

    public PaymentWebhookController(PaymentWebhookService paymentWebhookService, ObjectMapper objectMapper) {
        this.paymentWebhookService = paymentWebhookService;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/payments")
    public ResponseEntity<PaymentWebhookResponse> handlePaymentWebhook(
            @RequestHeader("X-Webhook-Signature") String signature,
            @RequestBody String rawPayload
    ) throws Exception {
        PaymentWebhookRequest request = objectMapper.readValue(rawPayload, PaymentWebhookRequest.class);
        PaymentWebhookResponse response = paymentWebhookService.processWebhook(request, rawPayload, signature);
        return ResponseEntity.ok(response);
    }
}
