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
@Table(name = "sce_exam_program", schema = "sce_course")
public class ExamProgram {

	@Id
	private int exam_program_id;
	private int stream_id;
	@Column(name = "exam_program_name")
	private String examProgramName;
	private int acdc_id;
	private int class_id;
	private String target_exam;
	private Integer status;

	@ManyToOne
	@JoinColumn(name = "program_id")
	private ProgramName programName;
}
