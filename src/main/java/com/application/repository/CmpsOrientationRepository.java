package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.CmpsOrientation;
import com.application.entity.Orientation;

@Repository
public interface CmpsOrientationRepository extends JpaRepository<CmpsOrientation, Integer>{

    Optional<CmpsOrientation> findByOrientation(Orientation orientation);
    
    List<CmpsOrientation> findByCmpsIdAndOrientationOrientationId(int cmpsId, int orientationId);
    

	 
	 
}
