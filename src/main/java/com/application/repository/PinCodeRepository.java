package com.application.repository;


import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.PinCodeLocationDTO;
import com.application.entity.PinCode;

@Repository
public interface PinCodeRepository extends JpaRepository<PinCode, Integer>{
	
	@Query("SELECT DISTINCT new com.application.dto.PinCodeLocationDTO(" +
	           "s.stateId, s.stateName, d.districtId, d.districtName) " +
	           "FROM PinCode p " +
	           "JOIN p.state s " +
	           "JOIN p.district d " +
	           "WHERE p.pin_code = :pinCode")
	    Optional<PinCodeLocationDTO> findStateAndDistrictByPinCode(@Param("pinCode") int pinCode);
}
