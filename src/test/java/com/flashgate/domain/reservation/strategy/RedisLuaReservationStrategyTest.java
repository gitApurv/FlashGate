package com.flashgate.domain.reservation.strategy;

import com.flashgate.common.exception.AlreadyReservedException;
import com.flashgate.common.exception.SoldOutException;
import com.flashgate.domain.campaign.dto.CampaignCacheDto;
import com.flashgate.domain.campaign.service.CampaignCacheService;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.event.ReservationKafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
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
class RedisLuaReservationStrategyTest {

    @Autowired
    private RedisLuaReservationStrategy strategy;

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @MockitoBean
    private CampaignCacheService campaignCacheService;

    @MockitoBean
    private ReservationKafkaProducer kafkaProducer;

    private UUID campaignId;
    private UUID userId;

    @BeforeEach
    void setUp() {
        campaignId = UUID.randomUUID();
        userId = UUID.randomUUID();

        when(campaignCacheService.isSaleActive(campaignId)).thenReturn(true);
        when(campaignCacheService.getCampaignWindow(campaignId)).thenReturn(
                new CampaignCacheDto(campaignId, Instant.now().minus(10, ChronoUnit.MINUTES), Instant.now().plus(1, ChronoUnit.HOURS))
        );
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldSuccessfullyReserveAndPublishToKafkaWhenLuaReturnsOne() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("1");

        ReservationRequest request = new ReservationRequest(
                campaignId, userId, "idemp-" + UUID.randomUUID(), ConcurrencyMode.REDIS_KAFKA_PIPELINE
        );

        ReservationResponse response = strategy.reserve(request);

        assertNotNull(response);
        assertEquals(campaignId, response.campaignId());
        assertEquals(userId, response.userId());
        assertEquals("QUEUED_FOR_CONFIRMATION", response.status());

        // Verify Kafka event was dispatched
        verify(kafkaProducer).publishReservedEvent(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowSoldOutExceptionWhenLuaReturnsZero() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("0");

        ReservationRequest request = new ReservationRequest(
                campaignId, userId, "idemp-" + UUID.randomUUID(), ConcurrencyMode.REDIS_KAFKA_PIPELINE
        );

        assertThrows(SoldOutException.class, () -> strategy.reserve(request));
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldThrowAlreadyReservedExceptionWhenLuaReturnsMinusOne() {
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn("-1");

        ReservationRequest request = new ReservationRequest(
                campaignId, userId, "idemp-" + UUID.randomUUID(), ConcurrencyMode.REDIS_KAFKA_PIPELINE
        );

        assertThrows(AlreadyReservedException.class, () -> strategy.reserve(request));
        verifyNoInteractions(kafkaProducer);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnExistingReservationWhenIdempotencyHit() {
        UUID existingReservationId = UUID.randomUUID();
        when(redisTemplate.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                .thenReturn(existingReservationId.toString());

        ReservationRequest request = new ReservationRequest(
                campaignId, userId, "idemp-existing", ConcurrencyMode.REDIS_KAFKA_PIPELINE
        );

        ReservationResponse response = strategy.reserve(request);
        assertEquals(existingReservationId, response.reservationId());
        verifyNoInteractions(kafkaProducer);
    }
}
