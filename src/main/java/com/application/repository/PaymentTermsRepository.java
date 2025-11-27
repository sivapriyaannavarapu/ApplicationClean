package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.PaymentTerms;

@Repository
public interface PaymentTermsRepository extends JpaRepository<PaymentTerms, Integer>{

}
