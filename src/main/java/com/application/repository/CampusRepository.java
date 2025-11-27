package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.CampusDetailsDTO;
import com.application.dto.CampusDropdownDTO;
import com.application.dto.CampusDto;
import com.application.dto.GenericDropdownDTO;
import com.application.entity.Campus;
import com.application.entity.Zone;

@Repository
public interface CampusRepository extends JpaRepository<Campus, Integer>{
	
	
	@Query("SELECT za.campus FROM ZonalAccountant za WHERE za.zone.zoneId = :zoneId AND za.isActive = 1")
    List<Campus> findActiveCampusesByZoneId(@Param("zoneId") int zoneId);
	
	List<Campus> findByCityCityId(int cityId);
	
	@Query(value = """
	        SELECT c.* 
	        FROM sce_campus.sce_cmps c 
	        JOIN sce_locations.sce_campaign cam 
	          ON c.cmps_id = cam.cmps_id 
	        WHERE cam.campaign_id = :campaignId
	        """, nativeQuery = true)
	    List<Campus> findCampusByCampaignId(@Param("campaignId") int campaignId);
	 
	 @Query("SELECT new com.application.dto.CampusDto(c.campusId, c.campusName) FROM Campus c WHERE c.zone = :zone")
	    List<CampusDto> findByZoneAsDto(@Param("zone") Zone zone);
	 
	 @Query("SELECT new com.application.dto.CampusDetailsDTO(c.cmps_type, city.cityId, city.cityName) " +
	           "FROM Campus c " +
	           "JOIN c.city city " +
	           "WHERE c.campusId = :campusId")
	    Optional<CampusDetailsDTO> findCampusDetailsById(@Param("campusId") int campusId);
	 
	 @Query("SELECT new com.application.dto.CampusDropdownDTO(c.campusId, c.campusName) " +
	           "FROM Campus c JOIN c.businessType bt " +
	           // --- FIX: Compare uppercase versions ---
	           "WHERE UPPER(bt.businessTypeName) = UPPER(:businessTypeName) AND c.isActive = 1 " +
	           "ORDER BY c.campusName ASC")
	    List<CampusDropdownDTO> findActiveCampusesByBusinessTypeName(@Param("businessTypeName") String businessTypeName);
	 
	 @Query("SELECT NEW com.application.dto.GenericDropdownDTO(c.campusId, c.campusName) " +
	           "FROM Campus c " +
	           "WHERE c.isActive = 1")
	    List<GenericDropdownDTO> findAllActiveCampusesForDropdown();
	 
	 @Query("SELECT new com.application.dto.GenericDropdownDTO(c.campusId, c.campusName) "
	         + "FROM Campus c WHERE c.campusId IN :campusIds AND c.isActive = 1")
	    List<com.application.dto.GenericDropdownDTO> findActiveCampusesByIds(List<Integer> campusIds);
	 
	 @Query("SELECT c FROM Campus c WHERE c.businessType.businessTypeId = :businessId AND c.zone.zoneId = :zoneId")
	 List<Campus> findSchoolCampusesByZone(@Param("businessId") int businessId,
	                                       @Param("zoneId") int zoneId);
 
	 @Query("SELECT c FROM Campus c WHERE c.businessType.businessTypeId = :businessId")
	 List<Campus> findSchoolCampuses(@Param("businessId") int businessId);
 
	 @Query("SELECT c FROM Campus c WHERE c.businessType.businessTypeId = :businessId")
	 List<Campus> findCollegeCampuses(@Param("businessId") int businessId);
	 
	 @Query("SELECT new com.application.dto.GenericDropdownDTO(c.campusId, c.campusName) " +
	           "FROM Campus c WHERE c.city.cityId = :cityId AND c.isActive = 1")
	    List<GenericDropdownDTO> findCampusesByCityId(@Param("cityId") int cityId);
	
}
