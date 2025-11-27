package com.application.entity;
import org.hibernate.annotations.Immutable;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
@Entity
@Data
@Immutable
@Table(name="sce_emp_view", schema="sce_admin")
public class SCEmployeeEntity {
	@Id
	@Column(name="emp_id")
	private int empId;
	@Column(name ="first_name")
	private String firstName;
	@Column(name ="last_name")
	private String lastName;
	@Column(name = "email")
	private String email;
	@Column(name = "password")
	private String password;
	@Column(name = "designation_name")
	private String designationName;
 
    // --- NEW FIELD ---
	@Column(name = "emp_campus_id")
	private int empCampusId;
	@Column(name= "emp_campus")
	private String campusName;
	@Column(name="category_name")
	private String category;
    // --- NEW FIELDS ---
	@Column(name = "zone_id")
	private int zoneId;
 
	@Column(name = "zone_name")
	private String zoneName;
	@Column(name = "emp_stud_application_role")
	private String empStudApplicationRole;
    // ------------------
	@Column(name = "is_active")
	private int isActive;
}