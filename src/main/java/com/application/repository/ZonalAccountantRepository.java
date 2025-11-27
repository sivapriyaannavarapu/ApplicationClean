package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.ZonalAccountant;

@Repository
public interface ZonalAccountantRepository extends JpaRepository<ZonalAccountant, Integer>{
	 
	 List<ZonalAccountant> findByZoneZoneIdAndIsActive(int zoneId, int isActive);
	 
	 @Query("SELECT za.zone.zoneId FROM ZonalAccountant za WHERE za.employee.id = :empId AND za.isActive = 1")
	 List<Integer> findZoneIdByEmployeeId(@Param("empId") int empId);
	 
	 @Query("SELECT za FROM ZonalAccountant za WHERE za.employee.emp_id = :empId AND za.isActive = 1")
	 Optional<ZonalAccountant> findByEmployeeEmpId(@Param("empId") int empId);
	 
	
}
