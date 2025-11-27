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
@Table(name="school_details" , schema = "sce_campus")
public class SchoolDetails {
	
	@Id
	private String school_name;
	private String school_type;
	private String sc_code;
	private String belong_school;
	private String board;
	private String grade;
	private String code;
	private String sch_grp;
	private String new_sc_code;
	private String state_name;
	private String district_name;
	private String new_district_name;
}
