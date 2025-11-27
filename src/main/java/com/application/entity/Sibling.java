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
@Table(name="sce_stud_sibling" , schema = "sce_student")
public class Sibling {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_sibling_id;
	private String sibling_name;
	private String sibling_school;
	private int created_by;

	
	@ManyToOne
	@JoinColumn(name = "stud_adms_id")
	private StudentAcademicDetails studentAcademicDetails;
	
	@ManyToOne
	@JoinColumn(name = "student_sibling_type" , referencedColumnName = "relation_id")
	private StudentRelation studentRelation;
	
	@ManyToOne
	@JoinColumn(name = "gender_id")
	private Gender gender;
	
	@ManyToOne
	@JoinColumn(name = "sibling_class_id" , referencedColumnName = "class_id")
	private StudentClass studentClass;
}