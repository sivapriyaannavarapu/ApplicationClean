package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
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
@Table(name = "sce_cmps", schema = "sce_campus")
public class Campus {

	@Id
	@Column(name = "cmps_id")
	private int campusId;

	@Column(name = "cmps_name")
	private String campusName;
	
	private String cmps_type;
	private String cmps_code;

	@ManyToOne
	@JoinColumn(name = "city_id")
	private City city;

	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;

	@ManyToOne
	@JoinColumn(name = "zone_id")
	private Zone zone;
	
	@ManyToOne(fetch = FetchType.LAZY)
    // --- FIX: Use the correct foreign key column name ---
    @JoinColumn(name = "business_id") // Changed from business_type_id
    private BusinessType businessType;
    // ---
 
    @Column(name = "is_active")
    private Integer isActive;
}
