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
@Table(name = "sce_section", schema = "sce_student")
public class Section {

	@Id
	private int section_id;
	@Column(name = "section_name")
	private String sectionName;

//	@ManyToOne
//	@JoinColumn(name = "cmps_id")
//	private Campus campus;

	@ManyToOne
	@JoinColumn(name = "class_id")
	private StudentClass studentClass;

}
