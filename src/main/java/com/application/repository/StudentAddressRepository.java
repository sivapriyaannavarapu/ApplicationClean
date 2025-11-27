package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.StudentAcademicDetails;
import com.application.entity.StudentAddress;

@Repository
public interface StudentAddressRepository extends JpaRepository<StudentAddress, Integer>{

	Optional<StudentAddress> findByStudentAcademicDetails(StudentAcademicDetails studentAcademicDetails);	
}
