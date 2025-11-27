package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.OrgBankBranch;

@Repository
public interface OrgBankBranchRepository extends JpaRepository<OrgBankBranch, Integer>{

}
