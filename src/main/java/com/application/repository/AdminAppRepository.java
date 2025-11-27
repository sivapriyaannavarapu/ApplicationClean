package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.AppRangeDTO;
import com.application.entity.AdminApp;

@Repository
public interface AdminAppRepository extends JpaRepository<AdminApp, Integer> {

    // 1️⃣ Get amounts
    @Query("""
        SELECT DISTINCT a.app_amount 
        FROM AdminApp a 
        WHERE a.employee.id = :empId 
          AND a.academicYear.id = :academicYearId 
          AND a.is_active = 1
    """)
    List<Integer> findAmountsByEmpIdAndAcademicYear(
            @Param("empId") int empId,
            @Param("academicYearId") int academicYearId
    );

    // 4️⃣ Validation for application number range
    @Query("""
        SELECT a FROM AdminApp a
        WHERE :applicationNo BETWEEN a.appFromNo AND a.appToNo
          AND a.academicYear.id = :academicYearId
          AND a.is_active = 1
    """)
    Optional<AdminApp> findActiveAdminAppByAppNoAndAcademicYear(
            @Param("applicationNo") long applicationNo,
            @Param("academicYearId") int academicYearId
    );

    // 6️⃣ Find by emp + year + amount
    @Query("""
        SELECT a FROM AdminApp a
        WHERE a.employee.id = :empId
          AND a.academicYear.id = :yearId
          AND (a.app_amount = :amount OR a.app_fee = :amount)
          AND a.is_active = 1
    """)
    Optional<AdminApp> findByEmpAndYearAndAmount(
            @Param("empId") int empId,
            @Param("yearId") int yearId,
            @Param("amount") Float amount
    );
}
