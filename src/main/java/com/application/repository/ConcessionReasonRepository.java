package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ConcessionReason;

@Repository
public interface ConcessionReasonRepository extends JpaRepository<ConcessionReason, Integer> {

}
