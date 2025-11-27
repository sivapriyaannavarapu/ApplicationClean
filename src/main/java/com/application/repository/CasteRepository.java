package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Caste;

@Repository
public interface CasteRepository extends JpaRepository<Caste, Integer>{

}
