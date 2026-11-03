package com.flashgate.domain.reservation.event;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class ReservationKafkaProducer {

    public static final String TOPIC_TICKET_RESERVED = "ticket-reserved-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ReservationKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishReservedEvent(TicketReservedEvent event) {
        kafkaTemplate.send(TOPIC_TICKET_RESERVED, event.campaignId().toString(), event);
    }
}
