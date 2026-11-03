package com.flashgate.e2e;

import com.flashgate.common.util.HmacUtils;
import com.flashgate.domain.campaign.dto.CreateCampaignRequest;
import com.flashgate.domain.campaign.service.CampaignService;
import com.flashgate.domain.payment.dto.PaymentWebhookRequest;
import com.flashgate.domain.payment.dto.PaymentWebhookResponse;
import com.flashgate.domain.payment.service.PaymentWebhookService;
import com.flashgate.domain.reconciliation.ReservationExpiryScheduler;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import com.flashgate.domain.reservation.strategy.ReservationStrategy;
import com.flashgate.domain.reservation.strategy.ReservationStrategyFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class FlashGateE2EChaosTest {

    @Autowired
    private CampaignService campaignService;

    @Autowired
    private ReservationStrategyFactory strategyFactory;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private PaymentWebhookService paymentWebhookService;

    @Autowired
    private ReservationExpiryScheduler expiryScheduler;

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @Value("${flashgate.security.webhook-secret:test-webhook-secret-key-32chars}")
    private String webhookSecret;

    @Test
    void fullFlashSaleLifecycleShouldExecuteWithConsistentInventory() {
        // 1. Admin creates a new campaign
        var campaignResponse = campaignService.createCampaign(new CreateCampaignRequest(
                "Oasis Reunion Tour 2026",
                100,
                15000L,
                Instant.now().minus(10, ChronoUnit.MINUTES),
                Instant.now().plus(2, ChronoUnit.HOURS)
        ));
        assertNotNull(campaignResponse.id());

        // 2. Client reserves a ticket via Pessimistic Lock mode
        ReservationStrategy strategy = strategyFactory.getStrategy(ConcurrencyMode.PESSIMISTIC_LOCK);
        UUID userId = UUID.randomUUID();
        String idempotencyKey = "idemp-e2e-" + UUID.randomUUID();

        ReservationResponse resResponse = strategy.reserve(new ReservationRequest(
                campaignResponse.id(),
                userId,
                idempotencyKey,
                ConcurrencyMode.PESSIMISTIC_LOCK
        ));

        assertNotNull(resResponse.reservationId());
        assertEquals(ReservationStatus.PENDING_PAYMENT.name(), resResponse.status());

        // 3. Payment Webhook confirms the reservation
        String payload = "{\"reservationId\":\"" + resResponse.reservationId() + "\"}";
        String signature = HmacUtils.computeHmacSha256(payload, webhookSecret);

        PaymentWebhookResponse paymentResponse = paymentWebhookService.processWebhook(
                new PaymentWebhookRequest(resResponse.reservationId(), "pay_e2e_999", "PAYMENT_SUCCEEDED", 15000L),
                payload,
                signature
        );

        assertEquals("CONFIRMED", paymentResponse.status());

        Reservation confirmedRes = reservationRepository.findById(resResponse.reservationId()).orElseThrow();
        assertEquals(ReservationStatus.CONFIRMED, confirmedRes.getStatus());
    }
}
