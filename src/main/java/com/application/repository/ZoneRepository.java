package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import com.application.dto.GenericDropdownDTO;
import com.application.entity.Zone;

@Repository
public interface ZoneRepository extends JpaRepository<Zone, Integer>{

    List<Zone> findByCityCityId(int cityId);
    Optional<Zone> findByZoneNameIgnoreCase(String zoneName);
    
    @Query("SELECT NEW com.application.dto.GenericDropdownDTO(z.zoneId, z.zoneName) " +
	           "FROM Zone z ")
	    List<GenericDropdownDTO> findAllActiveZonesForDropdown();

}