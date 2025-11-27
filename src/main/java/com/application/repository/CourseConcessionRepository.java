package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.CourseConcession;

@Repository
public interface CourseConcessionRepository extends JpaRepository<CourseConcession, Integer>{

}
