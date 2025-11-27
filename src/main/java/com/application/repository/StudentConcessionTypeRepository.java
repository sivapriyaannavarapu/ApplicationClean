package com.application.repository;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.StudentConcessionType;

@Repository
public interface StudentConcessionTypeRepository extends JpaRepository<StudentConcessionType, Integer> {

	List<StudentConcessionType> findByStudAdmsId(int studAdmsId);
}
