package com.flashgate.domain.repository;

import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class SchemaAndEntityRepositoryTest {

    @Autowired
    private CampaignRepository campaignRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Test
    void shouldPersistAndRetrieveCampaignWithOptimisticVersion() {
        Campaign campaign = new Campaign();
        campaign.setId(UUID.randomUUID());
        campaign.setTitle("Coldplay World Tour 2026");
        campaign.setTotalStock(500);
        campaign.setAvailableStock(500);
        campaign.setPriceCents(9900L);
        campaign.setSaleStartsAt(Instant.now().minus(1, ChronoUnit.HOURS));
        campaign.setSaleEndsAt(Instant.now().plus(1, ChronoUnit.HOURS));

        Campaign saved = campaignRepository.saveAndFlush(campaign);
        assertNotNull(saved.getId());
        assertEquals(0L, saved.getVersion());

        // Test pessimistic lock query
        Optional<Campaign> locked = campaignRepository.findCampaignForUpdate(saved.getId());
        assertTrue(locked.isPresent());
        assertEquals("Coldplay World Tour 2026", locked.get().getTitle());
    }

    @Test
    void shouldPersistReservationAndFindExpiredForUpdate() {
        Campaign campaign = new Campaign();
        campaign.setId(UUID.randomUUID());
        campaign.setTitle("Test Campaign");
        campaign.setTotalStock(100);
        campaign.setAvailableStock(100);
        campaign.setPriceCents(5000L);
        campaign.setSaleStartsAt(Instant.now().minus(1, ChronoUnit.HOURS));
        campaign.setSaleEndsAt(Instant.now().plus(1, ChronoUnit.HOURS));
        campaignRepository.saveAndFlush(campaign);

        Reservation res = new Reservation();
        res.setId(UUID.randomUUID());
        res.setCampaignId(campaign.getId());
        res.setUserId(UUID.randomUUID());
        res.setIdempotencyKey("idemp-" + UUID.randomUUID());
        res.setStatus(ReservationStatus.PENDING_PAYMENT);
        res.setExpiresAt(Instant.now().minus(1, ChronoUnit.MINUTES)); // Expired

        reservationRepository.saveAndFlush(res);

        List<Reservation> expired = reservationRepository.findExpiredReservationsForUpdate(
                Instant.now(), PageRequest.of(0, 10));

        assertFalse(expired.isEmpty());
        assertEquals(res.getId(), expired.getFirst().getId());
    }
}
