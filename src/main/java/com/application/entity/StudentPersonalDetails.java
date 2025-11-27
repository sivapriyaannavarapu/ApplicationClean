package com.application.entity;

import java.util.Date;

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
@Table(name="sce_stud_personal_detls" , schema = "sce_student")
public class StudentPersonalDetails {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_personal_id;
//	private String father_name;
//	private String mother_name;
//	private Long parent_mobile_no;
//	private String parent_mail;
	private Long stud_aadhaar_no;
//	private String occupation;
	private int is_active;
	private int created_by;
	private Date dob;
//	private String mail;
//	private int mail_is_verified;
//	private int caste_id;
//	private int religion_id;
	
	@ManyToOne
	@JoinColumn(name = "food_type_id")
	private FoodType foodType;
	
	@ManyToOne
	@JoinColumn(name = "caste_id")
	private Caste caste;
	
	@ManyToOne
	@JoinColumn(name = "religion_id")
	private Religion religion;
	
	@ManyToOne
	@JoinColumn(name = "stud_adms_id")
	private StudentAcademicDetails studentAcademicDetails;
	
	@ManyToOne
	@JoinColumn(name = "blood_group_id")
	private BloodGroup bloodGroup;
	

}