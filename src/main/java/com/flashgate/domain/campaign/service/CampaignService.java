package com.flashgate.domain.campaign.service;

import com.flashgate.common.exception.ReservationNotFoundException;
import com.flashgate.domain.campaign.dto.CampaignCacheDto;
import com.flashgate.domain.campaign.dto.CampaignResponse;
import com.flashgate.domain.campaign.dto.CreateCampaignRequest;
import com.flashgate.domain.campaign.dto.WarmupResponse;
import com.flashgate.domain.campaign.entity.Campaign;
import com.flashgate.domain.campaign.repository.CampaignRepository;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class CampaignService {

    private final CampaignRepository campaignRepository;
    private final CampaignCacheService campaignCacheService;
    private final StringRedisTemplate redisTemplate;

    public CampaignService(CampaignRepository campaignRepository,
                           CampaignCacheService campaignCacheService,
                           StringRedisTemplate redisTemplate) {
        this.campaignRepository = campaignRepository;
        this.campaignCacheService = campaignCacheService;
        this.redisTemplate = redisTemplate;
    }

    @Transactional
    public CampaignResponse createCampaign(CreateCampaignRequest request) {
        UUID campaignId = UUID.randomUUID();
        Campaign campaign = new Campaign(
                campaignId,
                request.title(),
                request.totalStock(),
                request.totalStock(), // availableStock starts equal to totalStock
                request.priceCents(),
                request.saleStartsAt(),
                request.saleEndsAt()
        );

        Campaign saved = campaignRepository.save(campaign);
        return toResponse(saved);
    }

    @Transactional(readOnly = true)
    public CampaignResponse getCampaign(UUID campaignId) {
        Campaign campaign = campaignRepository.findById(campaignId)
                .orElseThrow(() -> new ReservationNotFoundException("Campaign not found: " + campaignId));
        return toResponse(campaign);
    }

    @Transactional(readOnly = true)
    public WarmupResponse warmupCampaign(UUID campaignId) {
        Campaign campaign = campaignRepository.findById(campaignId)
                .orElseThrow(() -> new ReservationNotFoundException("Campaign not found: " + campaignId));

        // 1. Warmup Redis stock key
        String stockKey = "campaign:" + campaignId + ":stock";
        redisTemplate.opsForValue().set(stockKey, String.valueOf(campaign.getAvailableStock()));

        // 2. Warmup local Caffeine cache
        CampaignCacheDto cacheDto = new CampaignCacheDto(
                campaign.getId(),
                campaign.getSaleStartsAt(),
                campaign.getSaleEndsAt()
        );
        campaignCacheService.put(campaignId, cacheDto);

        return new WarmupResponse(
                campaign.getId(),
                campaign.getAvailableStock(),
                campaign.getSaleStartsAt(),
                campaign.getSaleEndsAt(),
                "CACHE_WARMED"
        );
    }

    private CampaignResponse toResponse(Campaign c) {
        return new CampaignResponse(
                c.getId(),
                c.getTitle(),
                c.getTotalStock(),
                c.getAvailableStock(),
                c.getPriceCents(),
                c.getSaleStartsAt(),
                c.getSaleEndsAt(),
                c.getCreatedAt()
        );
    }
}
