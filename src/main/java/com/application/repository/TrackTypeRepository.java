package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.TrackType;

@Repository
public interface TrackTypeRepository extends JpaRepository<TrackType, Integer>{

}
