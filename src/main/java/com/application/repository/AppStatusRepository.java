package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.AppStatus;

@Repository
public interface AppStatusRepository extends JpaRepository<AppStatus, Integer> {

    @Query("SELECT a FROM AppStatus a WHERE a.app_no = :appNo AND a.is_active = 1")
    Optional<AppStatus> findByApplicationNumber(@Param("appNo") int appNo);

    @Query("SELECT a FROM AppStatus a WHERE a.app_no = :appNo AND a.is_active = 1")
    Optional<AppStatus> findByApp_no(@Param("appNo") int appNo);
 
}
