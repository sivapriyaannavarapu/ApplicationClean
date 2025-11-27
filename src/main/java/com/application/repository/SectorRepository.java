package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Sector;

@Repository
public interface SectorRepository extends JpaRepository<Sector, Integer>{

}
