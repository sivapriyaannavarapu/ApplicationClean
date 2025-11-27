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
@Table(name="sce_payment_terms" , schema = "sce_course")
public class PaymentTerms {
	
	@Id
	private int payment_term_id;
	private String payment_term_type;
	private float amount;
	
	@ManyToOne
	@JoinColumn(name = "fee_head_detls_id" , referencedColumnName = "fee_head_combination_id")
	private FeeHeadDetails feeHeadDetails;
}
