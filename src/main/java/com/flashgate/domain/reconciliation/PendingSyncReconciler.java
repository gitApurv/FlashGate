package com.flashgate.domain.reconciliation;

import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.event.ReservationKafkaProducer;
import com.flashgate.domain.reservation.event.TicketReservedEvent;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Component
public class PendingSyncReconciler {

    private static final Logger log = LoggerFactory.getLogger(PendingSyncReconciler.class);

    private final CampaignRepository campaignRepository;
    private final ReservationRepository reservationRepository;
    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> releaseTicketScript;
    private final ReservationKafkaProducer kafkaProducer;

    public PendingSyncReconciler(CampaignRepository campaignRepository,
                                 ReservationRepository reservationRepository,
                                 StringRedisTemplate redisTemplate,
                                 DefaultRedisScript<Long> releaseTicketScript,
                                 ReservationKafkaProducer kafkaProducer) {
        this.campaignRepository = campaignRepository;
        this.reservationRepository = reservationRepository;
        this.redisTemplate = redisTemplate;
        this.releaseTicketScript = releaseTicketScript;
        this.kafkaProducer = kafkaProducer;
    }

    @Scheduled(cron = "${flashgate.reconciliation.sync-audit-cron:*/10 * * * * *}")
    public void reconcileOrphanedReservations() {
        List<Campaign> activeCampaigns = campaignRepository.findAll();
        long thresholdTimestamp = Instant.now().minus(30, ChronoUnit.SECONDS).toEpochMilli();

        for (Campaign campaign : activeCampaigns) {
            String pendingSyncKey = "campaign:" + campaign.getId() + ":pending_sync";
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(pendingSyncKey);

            if (entries.isEmpty()) {
                continue;
            }

            for (Map.Entry<Object, Object> entry : entries.entrySet()) {
                String reservationIdStr = entry.getKey().toString();
                String valStr = entry.getValue().toString(); // format: "userId:timestampMs"
                String[] parts = valStr.split(":");
                if (parts.length < 2) continue;

                String userIdStr = parts[0];
                long createdMs = Long.parseLong(parts[1]);

                if (createdMs < thresholdTimestamp) {
                    UUID reservationId = UUID.fromString(reservationIdStr);
                    Optional<Reservation> inDb = reservationRepository.findById(reservationId);

                    if (inDb.isPresent()) {
                        // DB persistence already succeeded, clean up hash
                        redisTemplate.opsForHash().delete(pendingSyncKey, reservationIdStr);
                    } else {
                        log.warn("Found orphaned reservation {} for user {}. Attempting re-dispatch.", reservationId, userIdStr);
                        // Re-publish to Kafka
                        TicketReservedEvent event = new TicketReservedEvent(
                                reservationId,
                                campaign.getId(),
                                UUID.fromString(userIdStr),
                                "reconciled-" + reservationId,
                                Instant.now().plus(10, ChronoUnit.MINUTES),
                                Instant.ofEpochMilli(createdMs)
                        );
                        kafkaProducer.publishReservedEvent(event);
                    }
                }
            }
        }
    }
}
