package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.GenericDropdownDTO;
import com.application.entity.CollegeMaster;

@Repository
public interface CollegeMasterRepository extends JpaRepository<CollegeMaster, Integer>{
	
	@Query("SELECT new com.application.dto.GenericDropdownDTO(c.college_master_id, c.college_name) " +
	           "FROM CollegeMaster c " +
	           "WHERE c.district2.districtId = :districtId")
	    List<GenericDropdownDTO> getCollegesByNewDistrict(@Param("districtId") Integer districtId);


}
