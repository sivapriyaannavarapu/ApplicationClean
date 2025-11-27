package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.AppFromDTO;
import com.application.dto.AppSeriesDTO;
import com.application.entity.AcademicYear;
import com.application.entity.BalanceTrack;

@Repository
public interface BalanceTrackRepository extends JpaRepository<BalanceTrack, Integer> {

    @Query("SELECT b FROM BalanceTrack b WHERE b.academicYear.acdcYearId = :academicYearId AND b.employee.id = :employeeId AND b.isActive = 1")
    Optional<BalanceTrack> findBalanceTrack(@Param("academicYearId") int academicYearId,
                                            @Param("employeeId") int employeeId);

    @Query("SELECT b FROM BalanceTrack b WHERE b.academicYear.acdcYearId = :academicYearId AND b.employee.id = :employeeId AND b.isActive = 1")
    List<BalanceTrack> findAppNumberRanges(@Param("academicYearId") int academicYearId,
                                           @Param("employeeId") int employeeId);

    @Query("SELECT bt FROM BalanceTrack bt WHERE :appNo BETWEEN bt.appFrom AND bt.appTo AND bt.isActive = 1 AND bt.issuedByType.appIssuedId = 4")
    Optional<BalanceTrack> findActiveBalanceTrackByAppNoRange(@Param("appNo") Long appNo);
    
    @Query("SELECT DISTINCT b.amount FROM BalanceTrack b WHERE b.employee.id = :empId AND b.academicYear.id = :academicYearId AND b.isActive = 1")
    List<Float> findAmountsByEmpIdAndAcademicYear(
        @Param("empId") int empId,
        @Param("academicYearId") int academicYearId // NEW PARAMETER
    );
  
     @Query("SELECT b FROM BalanceTrack b WHERE " +
	           "b.academicYear.acdcYearId = :yearId " +
	           "AND b.employee.emp_id = :empId " +
	           "AND b.isActive = 1 " +
	           "AND b.amount = :amount " +
	           "ORDER BY b.appFrom ASC")
	    List<BalanceTrack> findActiveBalancesByEmpAndAmount(
	            @Param("yearId") int yearId, 
	            @Param("empId") int empId, 
	            @Param("amount") Float amount
	    );
     
     @Query("SELECT b FROM BalanceTrack b WHERE " + "b.academicYear.acdcYearId = :yearId "
 			+ "AND b.employee.emp_id = :empId " + "AND b.amount = :amount " + "AND b.isActive = 1 "
 			+ "AND b.appTo = :targetEnd")
 	Optional<BalanceTrack> findMergeableRowForEmployee(@Param("yearId") int yearId, @Param("empId") int empId,
 			@Param("amount") Float amount, @Param("targetEnd") int targetEnd);
     
     @Query("SELECT new com.application.dto.AppSeriesDTO(concat(b.appFrom, ' - ', b.appTo), b.appFrom, b.appTo) " +
	           "FROM BalanceTrack b WHERE " +
	           "b.employee.emp_id = :empId " +
	           "AND b.amount = :amount " +
	           "AND b.isActive = 1 " +
	           "ORDER BY b.appFrom ASC")
	    List<AppSeriesDTO> findSeriesByEmpIdAndAmount(
	            @Param("empId") int empId, 
	            @Param("amount") Double amount
	    );
	
	@Query("SELECT new com.application.dto.AppSeriesDTO(concat(b.appFrom, ' - ', b.appTo), b.appFrom, b.appTo) " +
	           "FROM BalanceTrack b WHERE " +
	           "b.issuedToProId = :proId " +
	           "AND b.amount = :amount " +
	           "AND b.isActive = 1 " +
	           "ORDER BY b.appFrom ASC")
	    List<AppSeriesDTO> findSeriesByProIdAndAmount(
	            @Param("proId") int proId, 
	            @Param("amount") Double amount
	    );
	    
	
	@Query("SELECT b FROM BalanceTrack b WHERE " + "b.academicYear.acdcYearId = :yearId "
			+ "AND b.issuedToProId = :proId " + "AND b.isActive = 1 " + "AND b.amount = :amount")
	Optional<BalanceTrack> findActiveBalanceByProAndAmount(@Param("yearId") int yearId, @Param("proId") int proId,
			@Param("amount") Float amount);
	
	@Query("SELECT b FROM BalanceTrack b WHERE b.academicYear.acdcYearId = :yearId AND b.issuedToProId = :proId AND b.isActive = 1 AND b.amount = :amount ORDER BY b.appFrom ASC")
	List<BalanceTrack> findActiveBalancesByProAndAmount(@Param("yearId") int yearId, @Param("proId") int proId, @Param("amount") Float amount);
	
	@Query("SELECT b FROM BalanceTrack b WHERE " + "b.academicYear.acdcYearId = :yearId "
			+ "AND b.issuedToProId = :proId " + "AND b.amount = :amount " + "AND b.isActive = 1 "
			+ "AND b.appTo = :targetEnd")
	Optional<BalanceTrack> findMergeableRowForPro(@Param("yearId") int yearId, @Param("proId") int proId,
			@Param("amount") Float amount, @Param("targetEnd") int targetEnd);
	


}
