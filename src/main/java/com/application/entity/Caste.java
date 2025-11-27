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
@Table(name = "sce_caste" , schema = "sce_student")
public class Caste {
	
	
	@Id
	private int caste_id;
	private String caste_type;
//	private int is_active;
}
