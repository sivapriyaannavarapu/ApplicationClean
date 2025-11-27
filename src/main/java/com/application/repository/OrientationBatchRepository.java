package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.OrientationBatch;

@Repository
public interface OrientationBatchRepository extends JpaRepository<OrientationBatch, Integer>{
	
}
