package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.AcademicLanguage;

@Repository
public interface AcademicLanguageRepository extends JpaRepository<AcademicLanguage, Integer>{

}
