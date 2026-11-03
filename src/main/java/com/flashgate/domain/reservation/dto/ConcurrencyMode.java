package com.flashgate.domain.reservation.dto;

public enum ConcurrencyMode {
    NAIVE,
    OPTIMISTIC_LOCK,
    PESSIMISTIC_LOCK,
    REDIS_KAFKA_PIPELINE
}
