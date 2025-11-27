package com.application.entity;
 
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
 
@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name="sce_status", schema="sce_application")
public class ApplicationStatus {
	
	
	@Id
	private int status_id;
	private String status;
	
	@Column(name = "is_active")
	private int isActive;

	
}