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
@Table(name="sce_conc_reason" , schema = "sce_student")
public class ConcessionReason {
	
	@Id
	private int conc_reason_id;
	private String conc_reason;
}
