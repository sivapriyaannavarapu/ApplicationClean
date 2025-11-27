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
@Table(name="sce_cmps_orientation" , schema = "sce_course")
public class CmpsOrientation {
	
	
	@Id
	private int cmps_orientation_id;
	@Column(name = "cmps_id")
	private int cmpsId;
	@Column(name = "acdc_year_id")
	private Integer acdcYearId;
	private float orientation_fee;
	
	@ManyToOne
	@JoinColumn(name = "orientation_id")
	private Orientation orientation;
	
	@ManyToOne
	@JoinColumn(name = "orientation_batch_id")
	private OrientationBatch orientationBatch;
	
}
