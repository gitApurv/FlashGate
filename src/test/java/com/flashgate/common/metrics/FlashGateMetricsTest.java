package com.flashgate.common.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class FlashGateMetricsTest {

    @Autowired
    private FlashGateMetrics metrics;

    @Autowired
    private MeterRegistry meterRegistry;

    @Test
    void shouldRecordReservationMetricsByStrategyAndResult() {
        metrics.recordReservation("REDIS_KAFKA_PIPELINE", "SUCCESS", Duration.ofMillis(8));
        metrics.recordReservation("REDIS_KAFKA_PIPELINE", "SOLD_OUT", Duration.ofMillis(2));

        double successCount = meterRegistry.counter("flashgate.reservations.requests",
                "strategy", "REDIS_KAFKA_PIPELINE", "result", "SUCCESS").count();
        double soldOutCount = meterRegistry.counter("flashgate.reservations.requests",
                "strategy", "REDIS_KAFKA_PIPELINE", "result", "SOLD_OUT").count();

        assertEquals(1.0, successCount);
        assertEquals(1.0, soldOutCount);
    }
}
