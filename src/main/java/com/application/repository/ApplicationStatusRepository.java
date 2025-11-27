package com.application.repository;
 
import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ApplicationStatus;
 
@Repository
public interface ApplicationStatusRepository extends JpaRepository<ApplicationStatus, Integer> {

	
	List<ApplicationStatus> findByIsActive(int isActive);

}