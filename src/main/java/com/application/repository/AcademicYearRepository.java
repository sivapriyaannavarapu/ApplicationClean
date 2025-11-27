package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.AcademicYear;

@Repository
public interface AcademicYearRepository extends JpaRepository<AcademicYear, Integer>{
	
	 List<AcademicYear> findByAcdcYearIdIn(List<Integer> acdcYearIds);
	 Optional<AcademicYear> findByYear(Integer year);
	 
	 
	 
}