package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.application.entity.State;

@Repository
public interface StateRepository extends JpaRepository<State, Integer>{
	
	List<State> findByStatus(int is_active);
}
