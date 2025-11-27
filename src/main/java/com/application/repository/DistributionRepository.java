package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.Distribution;

@Repository
public interface DistributionRepository extends JpaRepository<Distribution, Integer> {

	@Query(value = "SELECT * FROM sce_application.sce_app_distrubution d WHERE d.created_by = :empId AND d.is_active = 1", nativeQuery = true)
	List<Distribution> findByCreatedBy(@Param("empId") int empId);

	@Query(value = "SELECT * FROM sce_application.sce_app_distrubution d WHERE d.created_by=:empId AND d.issued_to_type_id=:issuedToTypeId AND d.is_active=1",nativeQuery=true)
	List<Distribution> findByCreatedByAndIssuedToType(@Param("empId") int empId,
			@Param("issuedToTypeId") int issuedToTypeId);
	
	@Query("SELECT d FROM Distribution d WHERE d.academicYear.acdcYearId = :academicYearId AND d.appStartNo <= :endNo AND d.appEndNo >= :startNo AND d.isActive = 1")
	List<Distribution> findOverlappingDistributions(@Param("academicYearId") int academicYearId,
			@Param("startNo") int startNo, @Param("endNo") int endNo);

	@Query("SELECT d FROM Distribution d WHERE :admissionNo >= d.appStartNo AND :admissionNo <= d.appEndNo AND d.issuedToType.appIssuedId = 4 AND d.isActive = 1")
	Optional<Distribution> findProDistributionForAdmissionNumber(@Param("admissionNo") long admissionNo);
	
	@Query("SELECT SUM(d.totalAppCount) FROM Distribution d WHERE d.created_by = :empId AND d.academicYear.acdcYearId = :yearId AND d.amount = :amount AND d.isActive = 1")
	Optional<Integer> sumTotalAppCountByCreatedByAndAmount(@Param("empId") int empId, @Param("yearId") int yearId, @Param("amount") Float amount);
	
	@Query("SELECT d FROM Distribution d WHERE d.issued_to_emp_id = :empId AND d.academicYear.acdcYearId = :yearId AND d.amount = :amount AND d.isActive = 1 ORDER BY d.appStartNo ASC")
	List<Distribution> findActiveByIssuedToEmpIdAndAmountOrderByStart(
	    @Param("empId") int empId, 
	    @Param("yearId") int yearId, 
	    @Param("amount") Float amount
	);
	
	@Query("SELECT d.appDistributionId FROM Distribution d WHERE " +
	           "d.issued_to_emp_id = :empId " +
	           "AND d.appStartNo = :start " +
	           "AND d.appEndNo = :end " +
	           "AND d.amount = :amount " +
	           "AND d.isActive = 1")
	    Optional<Integer> findIdByEmpAndRange(
	            @Param("empId") int empId, 
	            @Param("start") int start, 
	            @Param("end") int end,
	            @Param("amount") Double amount
	    );

	    // 2. GET DISTRIBUTION ID FOR PRO
	    // Finds the active transaction ID for a specific PRO and Range
	    @Query("SELECT d.appDistributionId FROM Distribution d WHERE " +
	           "d.issued_to_pro_id = :proId " +
	           "AND d.appStartNo = :start " +
	           "AND d.appEndNo = :end " +
	           "AND d.amount = :amount " +
	           "AND d.isActive = 1")
	    Optional<Integer> findIdByProAndRange(
	            @Param("proId") int proId, 
	            @Param("start") int start, 
	            @Param("end") int end,
	            @Param("amount") Double amount
	    );
	    
	    @Query("SELECT SUM(d.totalAppCount) FROM Distribution d WHERE " +
	            "d.issued_to_pro_id = :proId " +
	            "AND d.academicYear.acdcYearId = :yearId " +
	            // "AND d.amount = :amount " +  <--- COMMENTED OUT
	            "AND d.isActive = 1")
	     Optional<Integer> sumTotalAppCountByIssuedToProIdAndAmount(
	             @Param("proId") int proId, 
	             @Param("yearId") int yearId, 
	             @Param("amount") Float amount // Keep param to avoid changing Service code, but don't use it
	     );
	    
	    @Query("SELECT MIN(d.appStartNo) FROM Distribution d WHERE d.issued_to_pro_id = :proId AND d.academicYear.acdcYearId = :yearId AND d.isActive = 1")
		Optional<Integer> findMinAppStartNoByIssuedToProId(@Param("proId") int proId, @Param("yearId") int yearId);

		// 3. Find Max End Number for PRO
		@Query("SELECT MAX(d.appEndNo) FROM Distribution d WHERE d.issued_to_pro_id = :proId AND d.academicYear.acdcYearId = :yearId AND d.isActive = 1")
		Optional<Integer> findMaxAppEndNoByIssuedToProId(@Param("proId") int proId, @Param("yearId") int yearId);

		@Query("SELECT d FROM Distribution d WHERE d.issued_to_emp_id = :empId AND d.academicYear.acdcYearId = :yearId AND d.isActive = 1 ORDER BY d.appStartNo ASC")
	    List<Distribution> findActiveHoldingsForEmp(@Param("empId") Integer empId, @Param("yearId") Integer yearId);
		

}
