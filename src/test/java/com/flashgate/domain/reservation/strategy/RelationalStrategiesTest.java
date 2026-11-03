package com.flashgate.domain.reservation.strategy;

import com.flashgate.common.exception.SoldOutException;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class RelationalStrategiesTest {

    @Autowired
    private CampaignRepository campaignRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ReservationStrategyFactory strategyFactory;

    private Campaign testCampaign;

    @BeforeEach
    void setUp() {
        reservationRepository.deleteAll();
        campaignRepository.deleteAll();

        Campaign campaign = new Campaign();
        campaign.setId(UUID.randomUUID());
        campaign.setTitle("Concurrency Benchmark Event");
        campaign.setTotalStock(5);
        campaign.setAvailableStock(5);
        campaign.setPriceCents(5000L);
        campaign.setSaleStartsAt(Instant.now().minus(1, ChronoUnit.HOURS));
        campaign.setSaleEndsAt(Instant.now().plus(1, ChronoUnit.HOURS));
        testCampaign = campaignRepository.saveAndFlush(campaign);
    }

    @Test
    void pessimisticStrategyShouldGuaranteeZeroOversellingUnderHighConcurrency() throws InterruptedException {
        ReservationStrategy strategy = strategyFactory.getStrategy(ConcurrencyMode.PESSIMISTIC_LOCK);
        int threadCount = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger soldOutCount = new AtomicInteger(0);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            UUID userId = UUID.randomUUID();
            String idempotencyKey = "idemp-" + UUID.randomUUID();
            futures.add(executor.submit(() -> {
                try {
                    latch.await();
                    ReservationResponse response = strategy.reserve(
                            new ReservationRequest(testCampaign.getId(), userId, idempotencyKey, ConcurrencyMode.PESSIMISTIC_LOCK)
                    );
                    if (response != null) {
                        successCount.incrementAndGet();
                    }
                } catch (SoldOutException e) {
                    soldOutCount.incrementAndGet();
                } catch (Exception e) {
                    // unexpected error
                }
            }));
        }

        latch.countDown();
        for (Future<?> f : futures) {
            try {
                f.get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {}
        }
        executor.shutdown();

        // Exactly initial stock (5) must succeed
        assertEquals(5, successCount.get(), "Pessimistic lock must allow exactly 5 reservations");
        assertEquals(15, soldOutCount.get(), "Remaining 15 must fail with SoldOutException");

        Campaign finalCampaign = campaignRepository.findById(testCampaign.getId()).orElseThrow();
        assertEquals(0, finalCampaign.getAvailableStock());
    }
}
