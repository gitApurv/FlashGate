package com.flashgate.domain.reservation.strategy;

import com.flashgate.common.exception.AlreadyReservedException;
import com.flashgate.common.exception.SaleClosedException;
import com.flashgate.common.exception.SoldOutException;
import com.flashgate.domain.campaign.service.CampaignCacheService;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.event.ReservationKafkaProducer;
import com.flashgate.domain.reservation.event.TicketReservedEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

@Service
public class RedisLuaReservationStrategy implements ReservationStrategy {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<String> reserveTicketScript;
    private final CampaignCacheService campaignCacheService;
    private final ReservationKafkaProducer kafkaProducer;

    public RedisLuaReservationStrategy(StringRedisTemplate redisTemplate,
                                       DefaultRedisScript<String> reserveTicketScript,
                                       CampaignCacheService campaignCacheService,
                                       ReservationKafkaProducer kafkaProducer) {
        this.redisTemplate = redisTemplate;
        this.reserveTicketScript = reserveTicketScript;
        this.campaignCacheService = campaignCacheService;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public ReservationResponse reserve(ReservationRequest request) {
        // 1. In-memory local clock check via Caffeine
        if (!campaignCacheService.isSaleActive(request.campaignId())) {
            throw new SaleClosedException("Flash sale window is closed");
        }

        UUID reservationId = UUID.randomUUID();
        Instant now = Instant.now();
        Instant expiresAt = now.plus(10, ChronoUnit.MINUTES);

        List<String> keys = List.of(
                "campaign:" + request.campaignId() + ":stock",
                "campaign:" + request.campaignId() + ":users",
                "idemp:" + request.idempotencyKey(),
                "campaign:" + request.campaignId() + ":pending_sync"
        );

        String result = redisTemplate.execute(
                reserveTicketScript,
                keys,
                request.userId().toString(),
                reservationId.toString(),
                String.valueOf(now.toEpochMilli())
        );

        if ("0".equals(result)) {
            throw new SoldOutException("Campaign is sold out");
        }
        if ("-1".equals(result)) {
            throw new AlreadyReservedException("User already reserved a ticket in this campaign");
        }

        // Check if an existing reservation ID was returned due to idempotency hit
        UUID effectiveReservationId = "1".equals(result) ? reservationId : UUID.fromString(result);

        if ("1".equals(result)) {
            // New reservation succeeded: publish async persistence event to Kafka
            TicketReservedEvent event = new TicketReservedEvent(
                    effectiveReservationId,
                    request.campaignId(),
                    request.userId(),
                    request.idempotencyKey(),
                    expiresAt,
                    now
            );
            kafkaProducer.publishReservedEvent(event);
        }

        return new ReservationResponse(
                effectiveReservationId,
                request.campaignId(),
                request.userId(),
                "QUEUED_FOR_CONFIRMATION",
                expiresAt,
                "/api/v1/reservations/" + effectiveReservationId
        );
    }

    @Override
    public ConcurrencyMode getMode() {
        return ConcurrencyMode.REDIS_KAFKA_PIPELINE;
    }
}
