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
@Table(name = "sce_dgm" , schema = "sce_application")
public class Dgm {
	
	@Id
	private int dgm_id;
	
	@ManyToOne
	@JoinColumn(name = "zone_id")
	private Zone zone;
	
	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;
	
	@ManyToOne
	@JoinColumn(name = "emp_id")
	private Employee employee;
	
	@ManyToOne
	@JoinColumn(name = "district_id")
	private District district;
	
	@Column(name = "is_active")
    private int isActive;
}
