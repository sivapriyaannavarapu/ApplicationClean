package com.application.entity;

import jakarta.persistence.Column;import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_conc_type" , schema = "sce_student")
public class ConcessionType {
	
	
	@Id	@Column(name = "conc_type_id")
	private int concTypeId;
	private String conc_type;	private Float general_conc_amount;
}
