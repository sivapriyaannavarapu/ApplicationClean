package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ProConcession;

@Repository
public interface ProConcessionRepository extends JpaRepository<ProConcession, Integer>{

}
