package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_organization" , schema = "sce_campus")
public class Organization {
	
	@Id
	@Column(name = "org_id")
    private int orgId;
	private String org_name;
	private String org_type;
	
}
