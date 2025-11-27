package com.application.entity;

import java.time.LocalDate;

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
@Table(name="sce_class_acdc_year" , schema = "sce_course")
public class ClassAcademeicYear {
	
	@Id
	private int class_acdc_year_id;
	private String join_year;
	private int acdc_id;
	private int status;
	private LocalDate start_date;
	private LocalDate end_date;
	
	@ManyToOne
	@JoinColumn(name = "class_id")
	private StudentClass studentClass;
}
