package com.flashgate.domain.payment.service;

import com.flashgate.common.exception.AlreadyReservedException;
import com.flashgate.common.exception.InvalidSignatureException;
import com.flashgate.common.exception.ReservationNotFoundException;
import com.flashgate.common.util.HmacUtils;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.payment.dto.PaymentWebhookRequest;
import com.flashgate.domain.payment.dto.PaymentWebhookResponse;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
public class PaymentWebhookService {

    private final ReservationRepository reservationRepository;
    private final CampaignRepository campaignRepository;
    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> releaseTicketScript;
    private final String webhookSecret;

    public PaymentWebhookService(ReservationRepository reservationRepository,
                                 CampaignRepository campaignRepository,
                                 StringRedisTemplate redisTemplate,
                                 DefaultRedisScript<Long> releaseTicketScript,
                                 @Value("${flashgate.security.webhook-secret:test-webhook-secret-key-32chars}") String webhookSecret) {
        this.reservationRepository = reservationRepository;
        this.campaignRepository = campaignRepository;
        this.redisTemplate = redisTemplate;
        this.releaseTicketScript = releaseTicketScript;
        this.webhookSecret = webhookSecret;
    }

    @Transactional
    public PaymentWebhookResponse processWebhook(PaymentWebhookRequest request, String rawPayload, String signature) {
        // 1. HMAC Signature Verification
        if (!HmacUtils.verifyHmacSha256(rawPayload, webhookSecret, signature)) {
            throw new InvalidSignatureException("Invalid HMAC signature for payment webhook");
        }

        // 2. Load reservation
        Reservation reservation = reservationRepository.findById(request.reservationId())
                .orElseThrow(() -> new ReservationNotFoundException("Reservation not found: " + request.reservationId()));

        // 3. State transition enforcement
        if (reservation.getStatus() == ReservationStatus.EXPIRED) {
            throw new AlreadyReservedException("Reservation has expired; payment rejected, refund required");
        }

        if (reservation.getStatus() == ReservationStatus.CONFIRMED) {
            return new PaymentWebhookResponse(reservation.getId(), "CONFIRMED", "Payment already confirmed");
        }

        if ("PAYMENT_SUCCEEDED".equalsIgnoreCase(request.eventType())) {
            reservation.setStatus(ReservationStatus.CONFIRMED);
            reservation.setUpdatedAt(Instant.now());
            reservationRepository.save(reservation);

            // Deduct from permanent PostgreSQL campaign counter
            Campaign campaign = campaignRepository.findById(reservation.getCampaignId()).orElse(null);
            if (campaign != null && campaign.getAvailableStock() > 0) {
                campaign.setAvailableStock(campaign.getAvailableStock() - 1);
                campaignRepository.save(campaign);
            }

            return new PaymentWebhookResponse(reservation.getId(), "CONFIRMED", "Payment processed successfully");
        } else {
            // Payment Failed: cancel and restock Redis
            reservation.setStatus(ReservationStatus.CANCELLED);
            reservation.setUpdatedAt(Instant.now());
            reservationRepository.save(reservation);

            List<String> keys = List.of(
                    "campaign:" + reservation.getCampaignId() + ":stock",
                    "campaign:" + reservation.getCampaignId() + ":users"
            );
            redisTemplate.execute(releaseTicketScript, keys, reservation.getUserId().toString());

            return new PaymentWebhookResponse(reservation.getId(), "CANCELLED", "Payment failed; stock released");
        }
    }
}
