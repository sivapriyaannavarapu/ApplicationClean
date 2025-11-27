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
@Table(name = "sce_emp_coupon" , schema = "sce_application")
public class EmployeeCoupon {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int emp_coupon_id;
	private int is_used;
//	private int is_active;

	@ManyToOne
	@JoinColumn(name = "emp_id")
	private Employee employee;

	@ManyToOne
	@JoinColumn(name = "application_coupon_id")
	private ApplicationCoupon applicationCoupon;

	@ManyToOne
	@JoinColumn(name = "acdc_id" , referencedColumnName = "acdc_year_id")
	private AcademicYear academicYear;
	
	

}
