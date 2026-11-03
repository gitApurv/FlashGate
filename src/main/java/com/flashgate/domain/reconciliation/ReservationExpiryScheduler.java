package com.flashgate.domain.reconciliation;

import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
public class ReservationExpiryScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReservationExpiryScheduler.class);

    private final ReservationRepository reservationRepository;
    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> releaseTicketScript;

    public ReservationExpiryScheduler(ReservationRepository reservationRepository,
                                      StringRedisTemplate redisTemplate,
                                      DefaultRedisScript<Long> releaseTicketScript) {
        this.reservationRepository = reservationRepository;
        this.redisTemplate = redisTemplate;
        this.releaseTicketScript = releaseTicketScript;
    }

    @Scheduled(cron = "${flashgate.reconciliation.expiry-cron:*/5 * * * * *}")
    @Transactional
    public int reclaimExpiredReservations() {
        Instant now = Instant.now();
        List<Reservation> expiredList = reservationRepository.findExpiredReservationsForUpdate(now, PageRequest.of(0, 100));

        if (expiredList.isEmpty()) {
            return 0;
        }

        log.info("Found {} expired reservations to reclaim and restock", expiredList.size());

        for (Reservation res : expiredList) {
            res.setStatus(ReservationStatus.EXPIRED);
            res.setUpdatedAt(now);

            // Execute compensating atomic release in Redis
            List<String> keys = List.of(
                    "campaign:" + res.getCampaignId() + ":stock",
                    "campaign:" + res.getCampaignId() + ":users"
            );
            redisTemplate.execute(releaseTicketScript, keys, res.getUserId().toString());
        }

        reservationRepository.saveAll(expiredList);
        return expiredList.size();
    }
}
