package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.AppIssuedType;

@Repository
public interface AppIssuedTypeRepository  extends JpaRepository<AppIssuedType, Integer>{

}
