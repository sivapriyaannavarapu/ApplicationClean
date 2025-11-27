package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Country;

@Repository
public interface CountryRepository extends JpaRepository<Country, Integer> {

}
