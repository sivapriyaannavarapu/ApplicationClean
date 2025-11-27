package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_board_schools" , schema = "sce_campus")
public class BoardSchools {
	
	@Id
	private int board_school_id;
	private String board_school_name;
	private String board_school_code;
	private String belong_school;
	private String board;
	private String grade;
	private String code;
	private String sch_group;
	private String new_sc_code;
	
	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;
	
	@ManyToOne
	@JoinColumn(name = "district_id")
	private District district;
	
	@ManyToOne
	@JoinColumn(name = "new_dist_id")
	private District district2;
	
	@ManyToOne
	@JoinColumn(name = "school_type_id")
	private CampusSchoolType campusSchoolType;
}
