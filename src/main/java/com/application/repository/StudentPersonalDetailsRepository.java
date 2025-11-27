package com.application.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.StudentAcademicDetails;
import com.application.entity.StudentPersonalDetails;

@Repository
public interface StudentPersonalDetailsRepository extends JpaRepository<StudentPersonalDetails, Integer> {

    Optional<StudentPersonalDetails> findByStudentAcademicDetails(StudentAcademicDetails academic);
}