package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.entity.ApplicationCoupon;
import com.application.entity.Employee;
import com.application.entity.EmployeeCoupon;

@Repository
public interface EmployeeCouponRepository extends JpaRepository<EmployeeCoupon, Integer>{
	
}
