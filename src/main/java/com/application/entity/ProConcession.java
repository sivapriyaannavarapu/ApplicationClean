package com.application.entity;

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
@Table(name = "sce_pro_conc" , schema = "sce_student")
public class ProConcession {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int pro_conc_id;
	private String adm_no;
	private String reason;
	private int conc_amount;
//	private int is_active;
	private int created_by;
	
	@ManyToOne
	@JoinColumn(name = "pro_emp_id" , referencedColumnName = "emp_id")
	private Employee employee;
}
