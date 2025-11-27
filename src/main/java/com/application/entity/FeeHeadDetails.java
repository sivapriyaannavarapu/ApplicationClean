package com.application.entity;

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
@Table(name="sce_fee_head_detl" , schema = "sce_course")
public class FeeHeadDetails {
	
	@Id
	private int fee_head_combination_id;
	private int acdc_year_id;
	private float amount;
	private int org_id;
	private int deduction_priority;
	private int conc_priority;
	
	@ManyToOne
	@JoinColumn(name = "fee_head_id")
	private FeeHead feeHead;
	
//	@ManyToOne
//	@JoinColumn(name = "fee_set_id")
//	private FeeSets feeSets;
	
}
