package com.application.entity;
 
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
@Table(name = "sce_org_bank_details" , schema = "sce_campus")
public class OrganizationBankDetails {
	
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int org_bank_details_id;
	private String account_name;
	private Long account_number;
	private String account_type;
	private String bank_name;
	private String bank_branch;
	private String ifsc_code;
	
	@ManyToOne
	@JoinColumn(name = "org_id")
	private Organization organization;
	
	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;
	
	@ManyToOne
	@JoinColumn(name="org_bank_id")
	private OrgBank orgBank;
	
	@ManyToOne
	@JoinColumn(name="org_bank_branch_id")
	private OrgBankBranch orgBankBranch;
}
 
 