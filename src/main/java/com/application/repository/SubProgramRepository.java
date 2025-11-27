package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.SubProgram;

@Repository
public interface SubProgramRepository extends JpaRepository<SubProgram, Integer>{

}
