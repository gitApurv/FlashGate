package com.flashgate.domain.campaign.controller;

import com.flashgate.domain.campaign.dto.CampaignResponse;
import com.flashgate.domain.campaign.dto.CreateCampaignRequest;
import com.flashgate.domain.campaign.dto.WarmupResponse;
import com.flashgate.domain.campaign.service.CampaignService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/campaigns")
public class CampaignAdminController {

    private final CampaignService campaignService;

    public CampaignAdminController(CampaignService campaignService) {
        this.campaignService = campaignService;
    }

    @PostMapping
    public ResponseEntity<CampaignResponse> createCampaign(@Valid @RequestBody CreateCampaignRequest request) {
        CampaignResponse response = campaignService.createCampaign(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{campaignId}")
    public ResponseEntity<CampaignResponse> getCampaign(@PathVariable UUID campaignId) {
        CampaignResponse response = campaignService.getCampaign(campaignId);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{campaignId}/warmup")
    public ResponseEntity<WarmupResponse> warmupCampaign(@PathVariable UUID campaignId) {
        WarmupResponse response = campaignService.warmupCampaign(campaignId);
        return ResponseEntity.ok(response);
    }
}
