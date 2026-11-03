package com.flashgate.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class FlashGateMetrics {

    private final MeterRegistry meterRegistry;

    public FlashGateMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordReservation(String strategy, String result, Duration duration) {
        Counter.builder("flashgate.reservations.requests")
                .description("Total flash sale reservation requests")
                .tag("strategy", strategy)
                .tag("result", result)
                .register(meterRegistry)
                .increment();

        Timer.builder("flashgate.reservations.latency")
                .description("Reservation processing duration")
                .tag("strategy", strategy)
                .register(meterRegistry)
                .record(duration);
    }

    public void recordRedisLuaLatency(Duration duration) {
        Timer.builder("flashgate.redis.lua.latency")
                .description("Redis Lua script execution duration")
                .register(meterRegistry)
                .record(duration);
    }

    public void recordKafkaPublishLatency(Duration duration) {
        Timer.builder("flashgate.kafka.publish.latency")
                .description("Kafka event dispatch duration")
                .register(meterRegistry)
                .record(duration);
    }
}
