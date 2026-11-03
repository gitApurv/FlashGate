package com.flashgate.domain.reservation.repository;

import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.entity.ReservationStatus;
import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ReservationRepository extends JpaRepository<Reservation, UUID> {

    Optional<Reservation> findByCampaignIdAndUserId(UUID campaignId, UUID userId);

    Optional<Reservation> findByIdempotencyKey(String idempotencyKey);

    long countByCampaignIdAndStatusIn(UUID campaignId, Collection<ReservationStatus> statuses);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")})
    @Query("SELECT r FROM Reservation r WHERE r.status = :status AND r.expiresAt <= :now ORDER BY r.expiresAt ASC")
    List<Reservation> findByStatusAndExpiresAtLessThanEqualForUpdate(
            @Param("status") ReservationStatus status,
            @Param("now") Instant now,
            Pageable pageable
    );

    default List<Reservation> findExpiredReservationsForUpdate(Instant now, Pageable pageable) {
        return findByStatusAndExpiresAtLessThanEqualForUpdate(ReservationStatus.PENDING_PAYMENT, now, pageable);
    }
}
