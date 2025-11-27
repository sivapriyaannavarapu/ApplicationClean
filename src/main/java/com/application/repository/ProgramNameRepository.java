package com.application.repository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.ProgramName;

@Repository
public interface ProgramNameRepository extends JpaRepository<ProgramName, Integer>{

}
