package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.District;

@Repository
public interface DistrictRepository  extends JpaRepository<District, Integer>{
	
	List<District> findByStateStateId(int stateId);
}
