package com.application.entity;

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
@Table(name = "sce_use_app_range" , schema = "sce_application")
public class UserAppRangeView {
	
	@Id
	private int emp_id;
	private int entity_id;
	private int acdc_year_id;
	private String range_start_no;
	private String range_end_no;
	private int total_app_count;
}
