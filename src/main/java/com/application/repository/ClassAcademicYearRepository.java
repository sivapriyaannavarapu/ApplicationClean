package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ClassAcademeicYear;

@Repository
public interface ClassAcademicYearRepository extends JpaRepository<ClassAcademeicYear, Integer>{

}
