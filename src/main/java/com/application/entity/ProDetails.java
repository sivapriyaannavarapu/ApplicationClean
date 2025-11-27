package com.application.entity;

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
@Table(name = "sce_pro_detl" , schema = "sce_employee")
public class ProDetails {

	
	@Id
	private int pro_detl_id;
	private String first_name;
	private String last_name;
	private Long primary_contact_no;
	private Long secondary_contact_no;
	private String email;
    private String adress;
    private int is_active;
    
    @ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;
	
}
