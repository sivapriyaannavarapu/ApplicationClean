package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name="sce_org_bank",schema = "sce_campus")
public class OrgBank {
	
	@Id
	private int org_bank_id;
	private String bank_name;
	private String ifsc_code;

}