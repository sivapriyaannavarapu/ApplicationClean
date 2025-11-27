package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.PaymentMode;

@Repository
public interface PaymentModeRepository extends JpaRepository<PaymentMode, Integer>{

}
