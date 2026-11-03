package com.flashgate.domain.campaign.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;

public record CreateCampaignRequest(
        @NotBlank(message = "Title is required")
        String title,

        @NotNull(message = "Total stock is required")
        @Min(value = 1, message = "Total stock must be at least 1")
        Integer totalStock,

        @NotNull(message = "Price is required")
        @Min(value = 0, message = "Price cannot be negative")
        Long priceCents,

        @NotNull(message = "Sale start time is required")
        Instant saleStartsAt,

        @NotNull(message = "Sale end time is required")
        Instant saleEndsAt
) {
    public CreateCampaignRequest {
        if (saleStartsAt != null && saleEndsAt != null && !saleEndsAt.isAfter(saleStartsAt)) {
            throw new IllegalArgumentException("saleEndsAt must be strictly after saleStartsAt");
        }
    }
}
