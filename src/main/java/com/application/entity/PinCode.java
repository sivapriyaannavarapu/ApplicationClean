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
@Table(name="sce_pin_code" , schema = "sce_locations")
public class PinCode {
	
	@Id
	private int pin_code_id;
	private String office_name;
	private String office_status;
	private int pin_code;
	private String telephone_no;
	private String taluk;
	private String postal_district_name;
	private String postal_division;
	private String postal_circle;
	private String postal_region;
	
	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;
	
	@ManyToOne
	@JoinColumn(name = "district_id")
	private District district;
}
