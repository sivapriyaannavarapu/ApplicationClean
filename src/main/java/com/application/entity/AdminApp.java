package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "sce_admin_app" , schema = "sce_application")
public class AdminApp {
	
	
	@Id
	private int admin_app_id;
	@Column(name = "app_from_no")
    private Integer appFromNo;

    @Column(name = "app_to_no")
    private Integer appToNo;

    @Column(name = "total_app")
    private Integer totalApp; // 2500
	private int app_amount;
	private Integer app_fee;
	private int is_active;
	
	@ManyToOne
	@JoinColumn(name = "state_id")
	private State state;

	@ManyToOne
	@JoinColumn(name = "emp_id")
	private Employee employee;
	
	@ManyToOne
	@JoinColumn(name = "acdc_year_id")
	private AcademicYear academicYear;


}
