package com.flashgate.domain.reservation.strategy;

import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import com.flashgate.domain.reservation.dto.ReservationRequest;
import com.flashgate.domain.reservation.dto.ReservationResponse;

public interface ReservationStrategy {

    ReservationResponse reserve(ReservationRequest request);

    ConcurrencyMode getMode();
}
