package com.application.entity;

import jakarta.annotation.Generated;
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
@Table(name = "sce_stud_coupon" , schema = "sce_application")
public class StudentCoupon {

	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_coupon_id;
	private int is_active;
	private int created_by;
	
	@ManyToOne
	@JoinColumn(name = "application_coupon_id")
	private ApplicationCoupon applicationCoupon;
	
	@ManyToOne
	@JoinColumn(name = "stud_adms_id")
	private StudentAcademicDetails studentAcademicDetails;

}
