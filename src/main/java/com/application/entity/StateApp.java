package com.application.entity;

import jakarta.persistence.Column;
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
@Table(name="sce_state_app" , schema = "sce_application")
public class StateApp {
	
	@Id
	private int state_app_id;
	private int total_no_of_app;
	private int app_start_no;
	private int app_end_no;
	private float amount;
	private int created_by;	
	private String app_fee;
	
	@Column(name = "is_active")
	private int isActive;
	
	@ManyToOne
	@JoinColumn(name = "acdc_year_id")
	private AcademicYear academicYear;
	
	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;
}
