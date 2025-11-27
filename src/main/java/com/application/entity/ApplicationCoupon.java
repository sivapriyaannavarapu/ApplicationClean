package com.application.entity;

import java.util.Date;

import jakarta.annotation.Generated;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_application_coupon" , schema = "sce_application")
public class ApplicationCoupon {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int application_coupon_id;
	private String coupon_code;
	private int coupon_discount;
	private int coupon_amount;
	private Date start_date;
	private Date end_date;
//	private int is_active;
}
