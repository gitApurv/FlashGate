package com.flashgate.domain.campaign;

import com.flashgate.domain.campaign.dto.CampaignCacheDto;
import com.flashgate.domain.campaign.dto.CampaignResponse;
import com.flashgate.domain.campaign.dto.CreateCampaignRequest;
import com.flashgate.domain.campaign.dto.WarmupResponse;
import com.flashgate.domain.campaign.service.CampaignCacheService;
import com.flashgate.domain.campaign.service.CampaignService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
class CampaignAdminServiceTest {

    @Autowired
    private CampaignService campaignService;

    @Autowired
    private CampaignCacheService campaignCacheService;

    @MockitoBean
    private StringRedisTemplate redisTemplate;

    @Test
    @SuppressWarnings("unchecked")
    void shouldCreateCampaignAndWarmupCache() {
        org.springframework.data.redis.core.ValueOperations<String, String> valueOps = mock(org.springframework.data.redis.core.ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);

        CreateCampaignRequest request = new CreateCampaignRequest(
                "Metallica Tour 2026",
                500,
                8500L,
                Instant.now().minus(5, ChronoUnit.MINUTES),
                Instant.now().plus(2, ChronoUnit.HOURS)
        );

        CampaignResponse response = campaignService.createCampaign(request);
        assertNotNull(response.id());
        assertEquals("Metallica Tour 2026", response.title());
        assertEquals(500, response.totalStock());
        assertEquals(500, response.availableStock());

        // Test Warmup
        WarmupResponse warmup = campaignService.warmupCampaign(response.id());
        assertEquals(response.id(), warmup.campaignId());
        assertEquals(500, warmup.warmedStock());
        assertEquals("CACHE_WARMED", warmup.status());

        // Verify Redis was invoked to set stock
        verify(redisTemplate).opsForValue();

        // Verify Caffeine Cache holds campaign window
        CampaignCacheDto cached = campaignCacheService.getCampaignWindow(response.id());
        assertNotNull(cached);
        assertTrue(campaignCacheService.isSaleActive(response.id()));
    }
}
