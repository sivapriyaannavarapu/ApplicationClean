package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.UserAdminView;

@Repository
public interface UserAdminViewRepository extends JpaRepository<UserAdminView, Integer>{
	
	@Query("SELECT u FROM UserAdminView u WHERE u.emp_id = :empId")
    List<UserAdminView> findRolesByEmpId(@Param("empId") int empId);
}
