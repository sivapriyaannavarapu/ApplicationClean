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
@Table(name = "sce_payment_mode" , schema = "sce_student")
public class PaymentMode {
	
	@Id
	private int payment_mode_id;
	private String payment_type;
//	private int is_active;
	private int created_by;
}
