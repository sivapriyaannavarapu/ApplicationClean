package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.OrgBank;

@Repository
public interface OrgBankRepository extends JpaRepository<OrgBank, Integer>{

}
