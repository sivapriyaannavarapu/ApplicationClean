package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.GenericDropdownDTO;
import com.application.entity.PreSchoolDetails;

@Repository
public interface PreSchoolDetailsRepository extends JpaRepository<PreSchoolDetails, Integer> {

	@Query("SELECT new com.application.dto.GenericDropdownDTO(p.school_id, p.school_name) " + "FROM PreSchoolDetails p "
			+ "WHERE p.district2.districtId = :districtId")
	List<GenericDropdownDTO> getSchoolsByDistrict(@Param("districtId") Integer districtId);
}
