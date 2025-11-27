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
@Table(name = "sce_stud_payment_detls", schema = "sce_student")
public class PaymentDetails {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_payment_detls_id;
	private float app_fee;
	private float conc_amount;
	private float paid_amount;
	private float due;
	private int created_by;
	private String pre_print_receipt_no; 
	private Date application_fee_pay_date;
	private String remarks;

	@ManyToOne
	@JoinColumn(name = "payment_mode_id")
	private PaymentMode paymenMode;

	@ManyToOne
	@JoinColumn(name = "stud_adms_id")
	private StudentAcademicDetails studentAcademicDetails;

	@ManyToOne
	@JoinColumn(name = "stud_conc_type_id")
	private StudentConcessionType studentConcessionType;

	@ManyToOne
	@JoinColumn(name = "acdc_year_id")
	private AcademicYear acedemicYear;

	@ManyToOne
	@JoinColumn(name = "class_id")
	private StudentClass studentClass;

	@ManyToOne
	@JoinColumn(name = "status_id")
	private Status status;
	
	 
}