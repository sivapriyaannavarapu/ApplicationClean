package com.application.entity;

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
@Table(name = "sce_org_bank_branch" , schema = "sce_campus")
public class OrgBankBranch {
	
	@Id
	private int org_bank_branch_id;
	private String branch_name;

}