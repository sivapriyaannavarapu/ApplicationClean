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
@Table(name = "sce_sub_program" , schema = "sce_course")
public class SubProgram {
	
	@Id
	private int sub_program_id;
	private String sub_program_name;
	private int acdc_id;
	private int status;
	
	@ManyToOne
	@JoinColumn(name = "program_id")
	private ProgramName programName;
}
