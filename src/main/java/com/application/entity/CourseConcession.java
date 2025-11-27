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
@Table(name="sce_conc" , schema = "sce_course")
public class CourseConcession {
	
	@Id
	private int conc_id;
	private int stud_id;
	private String year_of_conc;
	private float amount;
	
	@ManyToOne
	@JoinColumn(name = "fee_head_id")
	private FeeHead feeHead;
}
