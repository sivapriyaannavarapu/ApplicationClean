package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Orientation;

@Repository
public interface OrientationRepository extends JpaRepository<Orientation, Integer>{

}
