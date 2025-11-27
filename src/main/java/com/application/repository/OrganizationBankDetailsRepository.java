package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.OrgBank;
import com.application.entity.OrgBankBranch;
import com.application.entity.OrganizationBankDetails;

@Repository
public interface OrganizationBankDetailsRepository extends JpaRepository<OrganizationBankDetails, Integer> {

	@Query("SELECT DISTINCT obd.orgBank FROM OrganizationBankDetails obd WHERE obd.organization.orgId = :orgId")
	List<OrgBank> findDistinctBanksByOrganizationId(@Param("orgId") int orgId);

	@Query("SELECT DISTINCT obd.orgBankBranch FROM OrganizationBankDetails obd WHERE obd.organization.orgId = :orgId AND obd.orgBank.org_bank_id = :bankId")
	List<OrgBankBranch> findDistinctBranchesByOrganizationAndBankId(@Param("orgId") int orgId,
			@Param("bankId") int bankId);

	@Query("SELECT obd FROM OrganizationBankDetails obd WHERE obd.organization.orgId = :orgId AND obd.orgBank.org_bank_id = :bankId AND obd.orgBankBranch.org_bank_branch_id = :branchId")
	List<OrganizationBankDetails> findDetailsByAllIds(@Param("orgId") int orgId, @Param("bankId") int bankId,
			@Param("branchId") int branchId);

}
