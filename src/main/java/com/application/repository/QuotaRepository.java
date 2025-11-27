package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Quota;

@Repository
public interface QuotaRepository extends JpaRepository<Quota, Integer>{

}
