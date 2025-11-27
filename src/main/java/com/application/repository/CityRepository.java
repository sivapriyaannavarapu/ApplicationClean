package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.City;

@Repository
public interface CityRepository extends JpaRepository<City, Integer>{

	List<City> findByDistrictStateStateIdAndStatus(int stateId, int status);
    List<City> findByDistrictDistrictIdAndStatus(int districtId,int status);
    List<City> findByStatus(int status);
}