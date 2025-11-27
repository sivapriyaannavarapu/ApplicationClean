package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Sibling;
import com.application.entity.StudentAcademicDetails;

@Repository
public interface SiblingRepository extends JpaRepository<Sibling, Integer>{
	
	List<Sibling> findByStudentAcademicDetails(StudentAcademicDetails studentAcademicDetails);
}
