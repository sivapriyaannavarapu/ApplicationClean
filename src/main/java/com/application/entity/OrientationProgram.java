package com.application.entity;

import jakarta.annotation.Generated;
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
@Table(name = "sce_orientation_program" , schema = "sce_course")
public class OrientationProgram {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int orientation_program_id;
	
	@ManyToOne
	@JoinColumn(name = "orientation_id")
	private Orientation orientation;
	
	@ManyToOne
	@JoinColumn(name = "program_id")
	private ProgramName programName;
	
}
