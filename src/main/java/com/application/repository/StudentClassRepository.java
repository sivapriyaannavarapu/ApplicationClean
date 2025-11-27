package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.StudentClass;

@Repository
public interface StudentClassRepository extends JpaRepository<StudentClass, Integer>{
	
	List<StudentClass> findByIsActive(int isActive);
}
