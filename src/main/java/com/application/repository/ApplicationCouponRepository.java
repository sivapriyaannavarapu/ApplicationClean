package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.application.entity.ApplicationCoupon;

@Repository
public interface ApplicationCouponRepository extends JpaRepository<ApplicationCoupon, Integer> {
	
}
