package com.application.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.application.entity.Organization;

@Repository
public interface OrganizationRepository extends JpaRepository<Organization, Integer> {

}
