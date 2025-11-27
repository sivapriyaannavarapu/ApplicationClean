package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.BoardSchools;

@Repository
public interface BoardSchoolsRepository extends JpaRepository<BoardSchools, Integer>{

}
