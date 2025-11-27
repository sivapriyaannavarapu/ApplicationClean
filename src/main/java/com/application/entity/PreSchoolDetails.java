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
@Table(name = "sce_pre_school_details" , schema = "sce_campus")
public class PreSchoolDetails {
	
	@Id
	private int school_id;
	private String school_name;
	private String sc_code;
	private String belong_school;
	private String board;
	private String grade;
	private String code;
	
	@ManyToOne
	@JoinColumn(name = "school_type_id" , referencedColumnName = "study_type_id")
	private StudyType studyType;
	
	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;
	
	@ManyToOne
	@JoinColumn(name = "district_id")
	private District district;
	
	@ManyToOne
	@JoinColumn(name = "new_dist_id")
	private District district2;
}
