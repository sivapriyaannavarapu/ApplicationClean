	package com.application.entity;

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
@Table(name = "sce_school_type" , schema = "sce_campus")
public class CampusSchoolType {
	
	
	@Id
	private int school_type_id;
	private String school_type_name;
}
