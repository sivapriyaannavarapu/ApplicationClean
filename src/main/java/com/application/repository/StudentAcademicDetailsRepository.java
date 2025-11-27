package com.application.repository;
 
import java.util.Optional;
 
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
 
import com.application.entity.StudentAcademicDetails;
 
@Repository
public interface StudentAcademicDetailsRepository extends JpaRepository<StudentAcademicDetails, Integer> {
 
    Optional<StudentAcademicDetails> findByStudAdmsNo(Long admissionNo); 
}