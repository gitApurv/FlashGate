package com.flashgate.domain.reservation.event;

import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class ReservationKafkaConsumer {

    private final ReservationRepository reservationRepository;
    private final StringRedisTemplate redisTemplate;

    public ReservationKafkaConsumer(ReservationRepository reservationRepository, StringRedisTemplate redisTemplate) {
        this.reservationRepository = reservationRepository;
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(
            topics = ReservationKafkaProducer.TOPIC_TICKET_RESERVED,
            groupId = "flashgate-reservation-persisters",
            batch = "true"
    )
    @Transactional
    public void consumeReservationBatch(List<ConsumerRecord<String, TicketReservedEvent>> records, Acknowledgment ack) {
        if (records == null || records.isEmpty()) {
            if (ack != null) ack.acknowledge();
            return;
        }

        List<Reservation> reservationsToPersist = new ArrayList<>(records.size());
        for (ConsumerRecord<String, TicketReservedEvent> record : records) {
            TicketReservedEvent event = record.value();
            if (event != null) {
                Reservation reservation = new Reservation(
                        event.reservationId(),
                        event.campaignId(),
                        event.userId(),
                        event.idempotencyKey(),
                        ReservationStatus.PENDING_PAYMENT,
                        event.expiresAt()
                );
                reservation.setCreatedAt(event.createdAt() != null ? event.createdAt() : Instant.now());
                reservationsToPersist.add(reservation);
            }
        }

        // 1. Batch persist to PostgreSQL
        reservationRepository.saveAll(reservationsToPersist);

        // 2. Clean up pending_sync entries in Redis
        for (Reservation res : reservationsToPersist) {
            String pendingSyncKey = "campaign:" + res.getCampaignId() + ":pending_sync";
            redisTemplate.opsForHash().delete(pendingSyncKey, res.getId().toString());
        }

        // 3. Acknowledge Kafka message batch offset
        if (ack != null) {
            ack.acknowledge();
        }
    }
}
