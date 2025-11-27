package com.application.repository;
 
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.SCEmployeeEntity;
 
/**
* Repository for the sce_emp_view to find employee roles.
*/
@Repository
public interface SCEmployeeRepository extends JpaRepository<SCEmployeeEntity, Integer> {
	
	  List<SCEmployeeEntity> findByEmpId(int empId);
}