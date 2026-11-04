package com.flashgate.domain.campaign.repository;

import com.flashgate.domain.campaign.entity.Campaign;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CampaignRepository extends JpaRepository<Campaign, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT c FROM Campaign c WHERE c.id = :id")
    Optional<Campaign> findCampaignForUpdate(@Param("id") UUID id);

    @org.springframework.data.jpa.repository.Modifying
    @Query(value = "UPDATE campaigns SET available_stock = GREATEST(0, available_stock - 1) WHERE id = :id", nativeQuery = true)
    int decrementStockNaive(@Param("id") UUID id);
}
