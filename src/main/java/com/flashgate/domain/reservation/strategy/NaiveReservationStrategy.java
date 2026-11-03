package com.flashgate.domain.reservation.strategy;

import com.flashgate.common.exception.AlreadyReservedException;
import com.flashgate.common.exception.ReservationNotFoundException;
import com.flashgate.common.exception.SaleClosedException;
import com.flashgate.common.exception.SoldOutException;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.UUID;

@Service
public class NaiveReservationStrategy implements ReservationStrategy {

    private final CampaignRepository campaignRepository;
    private final ReservationRepository reservationRepository;

    public NaiveReservationStrategy(CampaignRepository campaignRepository, ReservationRepository reservationRepository) {
        this.campaignRepository = campaignRepository;
        this.reservationRepository = reservationRepository;
    }

    @Override
    @Transactional
    public ReservationResponse reserve(ReservationRequest request) {
        // 1. Idempotency check
        Optional<Reservation> existingIdemp = reservationRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existingIdemp.isPresent()) {
            Reservation r = existingIdemp.get();
            return ReservationResponse.from(r.getId(), r.getCampaignId(), r.getUserId(), r.getStatus().name(), r.getExpiresAt());
        }

        // 2. One-per-user check
        if (reservationRepository.findByCampaignIdAndUserId(request.campaignId(), request.userId()).isPresent()) {
            throw new AlreadyReservedException("User already reserved a ticket in this campaign");
        }

        // 3. Load campaign (no locking)
        Campaign campaign = campaignRepository.findById(request.campaignId())
                .orElseThrow(() -> new ReservationNotFoundException("Campaign not found: " + request.campaignId()));

        Instant now = Instant.now();
        if (now.isBefore(campaign.getSaleStartsAt()) || now.isAfter(campaign.getSaleEndsAt())) {
            throw new SaleClosedException("Flash sale window is closed");
        }

        // 4. Check available stock
        if (campaign.getAvailableStock() <= 0) {
            throw new SoldOutException("Campaign is sold out");
        }

        // 5. Decrement stock (vulnerable to lost updates under concurrency)
        campaign.setAvailableStock(campaign.getAvailableStock() - 1);
        campaignRepository.save(campaign);

        // 6. Create reservation
        UUID reservationId = UUID.randomUUID();
        Instant expiresAt = now.plus(10, ChronoUnit.MINUTES);
        Reservation reservation = new Reservation(
                reservationId,
                campaign.getId(),
                request.userId(),
                request.idempotencyKey(),
                ReservationStatus.PENDING_PAYMENT,
                expiresAt
        );
        reservationRepository.save(reservation);

        return ReservationResponse.from(reservationId, campaign.getId(), request.userId(), ReservationStatus.PENDING_PAYMENT.name(), expiresAt);
    }

    @Override
    public ConcurrencyMode getMode() {
        return ConcurrencyMode.NAIVE;
    }
}
