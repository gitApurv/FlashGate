package com.flashgate.domain.campaign.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.flashgate.common.exception.ReservationNotFoundException;
import com.flashgate.domain.campaign.dto.CampaignCacheDto;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
public class CampaignCacheService {

    private final CampaignRepository campaignRepository;
    private final Cache<UUID, CampaignCacheDto> cache;

    public CampaignCacheService(CampaignRepository campaignRepository) {
        this.campaignRepository = campaignRepository;
        this.cache = Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofHours(24))
                .build();
    }

    public void put(UUID campaignId, CampaignCacheDto dto) {
        cache.put(campaignId, dto);
    }

    public CampaignCacheDto getCampaignWindow(UUID campaignId) {
        return cache.get(campaignId, id -> {
            Campaign c = campaignRepository.findById(id)
                    .orElseThrow(() -> new ReservationNotFoundException("Campaign not found: " + id));
            return new CampaignCacheDto(c.getId(), c.getSaleStartsAt(), c.getSaleEndsAt());
        });
    }

    public boolean isSaleActive(UUID campaignId) {
        CampaignCacheDto window = getCampaignWindow(campaignId);
        Instant now = Instant.now();
        return !now.isBefore(window.saleStartsAt()) && !now.isAfter(window.saleEndsAt());
    }
}
