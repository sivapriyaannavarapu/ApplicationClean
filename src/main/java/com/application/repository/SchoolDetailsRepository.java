package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.SchoolDetails;

@Repository
public interface SchoolDetailsRepository extends JpaRepository<SchoolDetails, String> {
}