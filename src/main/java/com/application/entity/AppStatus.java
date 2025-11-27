package com.application.entity;

import java.time.LocalDate;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
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
@Table(name = "sce_app_status" , schema="sce_application")
public class AppStatus {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int app_status_id;
	private int app_no;
	private String reason;
	private int is_active;
	private Integer created_by;
	private Integer updated_by;
	private LocalDate updated_date;
	
	@ManyToOne
	@JoinColumn(name = "pro_id", referencedColumnName = "emp_id")
	private Employee employee;
	
	@ManyToOne
	@JoinColumn(name = "zone_id")
	private Zone zone;
	
	@ManyToOne
	@JoinColumn(name = "dgm_emp_id", referencedColumnName = "emp_id")
	private Employee employee2;

	@ManyToOne
	@JoinColumn(name = "status_id")
	private Status status;
	
	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;
	
}
