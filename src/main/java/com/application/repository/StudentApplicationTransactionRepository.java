package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import com.application.entity.StudentApplicationTransaction;

@Repository
public interface StudentApplicationTransactionRepository extends JpaRepository<StudentApplicationTransaction, Integer>{

}
