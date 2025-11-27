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
@Table(name = "sce_religion" , schema = "sce_student")
public class Religion {
	
	@Id
	private int religion_id;
	private String religion_type;
//	private int is_active;
}
