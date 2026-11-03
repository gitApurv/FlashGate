package com.flashgate.domain.reservation.controller;

import com.flashgate.common.exception.ReservationNotFoundException;
import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;
import com.flashgate.domain.reservation.entity.Reservation;
import com.flashgate.domain.reservation.repository.ReservationRepository;
import com.flashgate.domain.reservation.strategy.ReservationStrategy;
import com.flashgate.domain.reservation.strategy.ReservationStrategyFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class ReservationController {

    private final ReservationStrategyFactory strategyFactory;
    private final ReservationRepository reservationRepository;
    private final com.flashgate.common.metrics.FlashGateMetrics metrics;

    public ReservationController(ReservationStrategyFactory strategyFactory,
                                 ReservationRepository reservationRepository,
                                 com.flashgate.common.metrics.FlashGateMetrics metrics) {
        this.strategyFactory = strategyFactory;
        this.reservationRepository = reservationRepository;
        this.metrics = metrics;
    }

    @PostMapping("/campaigns/{campaignId}/reserve")
    public ResponseEntity<ReservationResponse> reserve(
            @PathVariable UUID campaignId,
            @RequestHeader("X-User-Id") UUID userId,
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "X-Concurrency-Mode", required = false) ConcurrencyMode concurrencyMode
    ) {
        long start = System.nanoTime();
        if (concurrencyMode == null) {
            concurrencyMode = ConcurrencyMode.REDIS_KAFKA_PIPELINE;
        }

        try {
            ReservationRequest request = new ReservationRequest(campaignId, userId, idempotencyKey, concurrencyMode);
            ReservationStrategy strategy = strategyFactory.getStrategy(concurrencyMode);
            ReservationResponse response = strategy.reserve(request);

            metrics.recordReservation(concurrencyMode.name(), "SUCCESS", java.time.Duration.ofNanos(System.nanoTime() - start));

            HttpStatus status = (concurrencyMode == ConcurrencyMode.REDIS_KAFKA_PIPELINE)
                    ? HttpStatus.ACCEPTED
                    : HttpStatus.OK;

            return ResponseEntity.status(status).body(response);
        } catch (com.flashgate.common.exception.SoldOutException e) {
            metrics.recordReservation(concurrencyMode.name(), "SOLD_OUT", java.time.Duration.ofNanos(System.nanoTime() - start));
            throw e;
        } catch (com.flashgate.common.exception.AlreadyReservedException e) {
            metrics.recordReservation(concurrencyMode.name(), "ALREADY_RESERVED", java.time.Duration.ofNanos(System.nanoTime() - start));
            throw e;
        } catch (com.flashgate.common.exception.SaleClosedException e) {
            metrics.recordReservation(concurrencyMode.name(), "CLOSED", java.time.Duration.ofNanos(System.nanoTime() - start));
            throw e;
        } catch (Exception e) {
            metrics.recordReservation(concurrencyMode.name(), "ERROR", java.time.Duration.ofNanos(System.nanoTime() - start));
            throw e;
        }
    }

    @GetMapping("/reservations/{reservationId}")
    public ResponseEntity<Reservation> getReservationStatus(@PathVariable UUID reservationId) {
        Reservation reservation = reservationRepository.findById(reservationId)
                .orElseThrow(() -> new ReservationNotFoundException("Reservation not found: " + reservationId));
        return ResponseEntity.ok(reservation);
    }
}
