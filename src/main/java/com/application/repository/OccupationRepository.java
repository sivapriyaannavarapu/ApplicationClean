package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Occupation;

@Repository
public interface OccupationRepository extends JpaRepository<Occupation, Integer>{

}
