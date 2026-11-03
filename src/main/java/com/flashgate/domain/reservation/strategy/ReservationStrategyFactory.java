package com.flashgate.domain.reservation.strategy;

import com.flashgate.domain.reservation.dto.ConcurrencyMode;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Component
public class ReservationStrategyFactory {

    private final Map<ConcurrencyMode, ReservationStrategy> strategies = new EnumMap<>(ConcurrencyMode.class);

    public ReservationStrategyFactory(List<ReservationStrategy> strategyList) {
        for (ReservationStrategy strategy : strategyList) {
            strategies.put(strategy.getMode(), strategy);
        }
    }

    public ReservationStrategy getStrategy(ConcurrencyMode mode) {
        ReservationStrategy strategy = strategies.get(mode);
        if (strategy == null) {
            throw new IllegalArgumentException("Unsupported concurrency mode: " + mode);
        }
        return strategy;
    }
}
