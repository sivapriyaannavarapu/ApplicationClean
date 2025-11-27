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
@Table(name="sce_zone_acct" , schema = "sce_application")
public class ZonalAccountant {

	@Id
	private int zone_acct_id;
	
	@Column(name = "is_active")
	private int isActive;

	@ManyToOne
	@JoinColumn(name = "zone_id")
	private Zone zone;

	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;

	@ManyToOne
	@JoinColumn(name = "emp_id")
	private Employee employee;
	
}
