package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.application.entity.StudyType;

@Repository
public interface StudyTypeRepository extends JpaRepository<StudyType, Integer>{

}
