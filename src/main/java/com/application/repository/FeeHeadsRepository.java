package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.FeeHead;

@Repository
public interface FeeHeadsRepository extends JpaRepository<FeeHead, Integer>{

}
