package com.application.repository;

import com.application.entity.UserAppRangeView;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserAppRangeViewRepository extends JpaRepository<UserAppRangeView, Integer> {
    
}