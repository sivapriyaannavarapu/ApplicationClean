package com.application.entity;

import java.time.LocalDate;

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
@Table(name = "sce_orientation_batch", schema = "sce_course")
public class OrientationBatch {

	@Id
	@Column(name = "orientation_batch_id")
	private int orientationBatchId;
	@Column(name = "orientation_batch_name")
	private String orientationBatchName;
	private LocalDate start_date;
	private LocalDate end_date;
	private int acdc_id;
	private int status;
	private String display_name;
	private int orientation_duration;
}
