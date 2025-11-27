package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.BloodGroup;

@Repository
public interface BloodGroupRepository extends JpaRepository<BloodGroup, Integer>{

}
