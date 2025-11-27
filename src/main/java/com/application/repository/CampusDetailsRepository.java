package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.CampusDetails;

@Repository
public interface CampusDetailsRepository extends JpaRepository<CampusDetails, Integer>{
	 
	 Optional<CampusDetails> findByCampusCampusIdAndAcademicYearAcdcYearId(int campusId, int academicYearId);
	 

}
