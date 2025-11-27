package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name="sce_cmps_detl" , schema = "sce_campus")
public class CampusDetails {
	
	@Id
	private int cmps_detl_id;
	private int education_type_id;
	private float app_fee;
	
	@ManyToOne
	@JoinColumn(name = "acdc_year_id")
	private AcademicYear academicYear;
	
	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;
}
