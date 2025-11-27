package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Language;

@Repository
public interface LanguageRepository extends JpaRepository<Language, Integer> {

}
