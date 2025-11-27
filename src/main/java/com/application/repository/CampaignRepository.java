package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Campaign;

@Repository
public interface CampaignRepository  extends JpaRepository<Campaign, Integer>{
	 List<Campaign> findByCity_CityId(int cityId);
	 
	 Campaign findByCampus_CampusId(int campusId);
	 
}
