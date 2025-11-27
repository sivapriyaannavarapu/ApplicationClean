package com.application.repository;

import com.application.entity.ParentOccupationView;
import com.application.entity.ParentOccupationViewId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ParentOccupationViewRepository extends JpaRepository<ParentOccupationView, ParentOccupationViewId> {

}