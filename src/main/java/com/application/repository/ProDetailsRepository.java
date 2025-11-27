package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ProDetails;

@Repository
public interface ProDetailsRepository extends JpaRepository<ProDetails, Integer>{

}
