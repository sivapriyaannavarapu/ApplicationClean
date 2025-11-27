package com.application.entity;

import jakarta.persistence.Column;
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
@Table(name = "sce_parent_detls" , schema = "sce_student")
public class ParentDetails {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name = "parent_detls_id")
	private int parentId;
	private String occupation;
	@Column(name = "mobile_no")
	private Long mobileNo;
	private String email;
	private String name;
	private int is_mail_varified;
	private int is_app_reg;
	private int created_by;
//	private int is_active;
	
	@ManyToOne
	@JoinColumn(name = "student_relation_id" , referencedColumnName = "relation_id")
	private StudentRelation studentRelation;	
	
	@ManyToOne
	@JoinColumn(name = "occupation_sector_id")
	private Sector sector;
	
	@ManyToOne
    @JoinColumn(name = "stud_adms_id") // This links to the student's ID
    private StudentAcademicDetails studentAcademicDetails;
}
