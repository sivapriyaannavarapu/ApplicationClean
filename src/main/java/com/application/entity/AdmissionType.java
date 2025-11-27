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
@Table(name = "sce_adms_type" , schema = "sce_student")
public class AdmissionType {
	
	@Id
	private int adms_type_id;
	private String adms_type_name;
}
