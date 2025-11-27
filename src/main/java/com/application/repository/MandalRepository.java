package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Mandal;

@Repository
public interface MandalRepository extends JpaRepository<Mandal, Integer>{
	
	List<Mandal> findByDistrictDistrictId(int districtId);
}
