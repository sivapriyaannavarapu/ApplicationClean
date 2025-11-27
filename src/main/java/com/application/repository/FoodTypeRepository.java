package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.FoodType;

@Repository
public interface FoodTypeRepository extends JpaRepository<FoodType, Integer>{

}
