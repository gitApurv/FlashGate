package com.flashgate.domain.reconciliation;

import com.flashgate.common.exception.AlreadyReservedException;
import com.flashgate.common.exception.InvalidSignatureException;
import com.flashgate.common.util.HmacUtils;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.payment.dto.PaymentWebhookRequest;
import com.flashgate.domain.payment.dto.PaymentWebhookResponse;
import com.flashgate.domain.payment.service.PaymentWebhookService;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class ReconciliationAndWebhookTest {

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private CampaignRepository campaignRepository;

    @Autowired
    private ReservationExpiryScheduler expiryScheduler;

    @Autowired
    private PaymentWebhookService paymentWebhookService;

    @Autowired
    private PendingSyncReconciler pendingSyncReconciler;

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @Value("${flashgate.security.webhook-secret:test-webhook-secret-key-32chars}")
    private String webhookSecret;

    private Campaign testCampaign;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        org.springframework.data.redis.core.HashOperations<String, Object, Object> hashOps = mock(org.springframework.data.redis.core.HashOperations.class);
        when(redisTemplate.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(java.util.Collections.emptyMap());

        reservationRepository.deleteAll();
        campaignRepository.deleteAll();

        Campaign campaign = new Campaign();
        campaign.setId(UUID.randomUUID());
        campaign.setTitle("Reconciliation Test Event");
        campaign.setTotalStock(10);
        campaign.setAvailableStock(10);
        campaign.setPriceCents(5000L);
        campaign.setSaleStartsAt(Instant.now().minus(1, ChronoUnit.HOURS));
        campaign.setSaleEndsAt(Instant.now().plus(1, ChronoUnit.HOURS));
        testCampaign = campaignRepository.saveAndFlush(campaign);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReclaimExpiredReservationsAndReleaseRedisStock() {
        Reservation res = new Reservation(
                UUID.randomUUID(),
                testCampaign.getId(),
                UUID.randomUUID(),
                "idemp-expire-test",
                ReservationStatus.PENDING_PAYMENT,
                Instant.now().minus(5, ChronoUnit.MINUTES) // Already expired
        );
        reservationRepository.saveAndFlush(res);

        when(redisTemplate.execute(any(DefaultRedisScript.class), anyList(), any())).thenReturn(1L);

        // Execute scheduled reclaim run
        int reclaimedCount = expiryScheduler.reclaimExpiredReservations();
        assertEquals(1, reclaimedCount);

        Reservation updated = reservationRepository.findById(res.getId()).orElseThrow();
        assertEquals(ReservationStatus.EXPIRED, updated.getStatus());

        // Verify release_ticket.lua was invoked with user_id
        verify(redisTemplate).execute(any(DefaultRedisScript.class), anyList(), eq(res.getUserId().toString()));
    }

    @Test
    void shouldInvokePendingSyncReconciler() {
        assertDoesNotThrow(() -> pendingSyncReconciler.reconcileOrphanedReservations());
        assertDoesNotThrow(() -> pendingSyncReconciler.rollbackOrphanedReservation(testCampaign.getId(), UUID.randomUUID(), UUID.randomUUID()));
    }

    @Test
    void shouldConfirmPaymentWithValidHmac() {
        Reservation res = new Reservation(
                UUID.randomUUID(),
                testCampaign.getId(),
                UUID.randomUUID(),
                "idemp-pay-test",
                ReservationStatus.PENDING_PAYMENT,
                Instant.now().plus(10, ChronoUnit.MINUTES)
        );
        reservationRepository.saveAndFlush(res);

        PaymentWebhookRequest request = new PaymentWebhookRequest(
                res.getId(),
                "pay_123456",
                "PAYMENT_SUCCEEDED",
                5000L
        );

        String payload = "{\"reservationId\":\"" + res.getId() + "\",\"paymentId\":\"pay_123456\",\"eventType\":\"PAYMENT_SUCCEEDED\",\"amountCents\":5000}";
        String signature = HmacUtils.computeHmacSha256(payload, webhookSecret);

        PaymentWebhookResponse response = paymentWebhookService.processWebhook(request, payload, signature);
        assertEquals("CONFIRMED", response.status());

        Reservation updated = reservationRepository.findById(res.getId()).orElseThrow();
        assertEquals(ReservationStatus.CONFIRMED, updated.getStatus());
    }

    @Test
    void shouldRejectPaymentWithInvalidHmacSignature() {
        PaymentWebhookRequest request = new PaymentWebhookRequest(
                UUID.randomUUID(),
                "pay_fake",
                "PAYMENT_SUCCEEDED",
                5000L
        );

        assertThrows(InvalidSignatureException.class, () ->
                paymentWebhookService.processWebhook(request, "{}", "invalid_signature")
        );
    }

    @Test
    void shouldRejectPaymentForExpiredReservationWithConflict() {
        Reservation res = new Reservation(
                UUID.randomUUID(),
                testCampaign.getId(),
                UUID.randomUUID(),
                "idemp-expired-pay",
                ReservationStatus.EXPIRED,
                Instant.now().minus(20, ChronoUnit.MINUTES)
        );
        reservationRepository.saveAndFlush(res);

        PaymentWebhookRequest request = new PaymentWebhookRequest(
                res.getId(),
                "pay_late",
                "PAYMENT_SUCCEEDED",
                5000L
        );

        String payload = "{\"reservationId\":\"" + res.getId() + "\"}";
        String signature = HmacUtils.computeHmacSha256(payload, webhookSecret);

        assertThrows(AlreadyReservedException.class, () ->
                paymentWebhookService.processWebhook(request, payload, signature)
        );
    }
}
