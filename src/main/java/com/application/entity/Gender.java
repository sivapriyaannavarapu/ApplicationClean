package com.application.entity;

import jakarta.persistence.Column;
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
@Table(name = "sce_gender", schema = "sce_student")
public class Gender {

	@Id
	private int gender_id;
	@Column(name = "gender_name")
	private String genderName;
	
	@Column(name = "is_active") 
    private int isActive;

}
