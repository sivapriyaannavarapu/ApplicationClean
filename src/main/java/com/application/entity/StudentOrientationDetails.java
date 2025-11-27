package com.application.entity;

import java.util.Date;

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
@Table(name = "sce_stud_orientation_detl" , schema = "sce_student")
public class StudentOrientationDetails {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_orientation_id;
	private Date orientation_date;
	
	@ManyToOne
	@JoinColumn(name = "stud_adms_id")
	private StudentAcademicDetails studentAcademicDetails;
	
	@ManyToOne
	@JoinColumn(name = "class_id")
	private StudentClass studentClass;
	
	@ManyToOne
	@JoinColumn(name = "section_id")
	private Section section;
	
	@ManyToOne
	@JoinColumn(name = "orientation_id")
	private Orientation orientation;
	
	@ManyToOne
	@JoinColumn(name = "orientation_batch_id")
	private OrientationBatch orientationBatch;
	
	@ManyToOne
	@JoinColumn(name = "stream_id")
	private Stream stream;
	
	@ManyToOne
	@JoinColumn(name = "program_id")
	private ProgramName programName;
	
	@ManyToOne
	@JoinColumn(name = "exam_program_id")
	private ExamProgram examProgram;
	
	@ManyToOne
	@JoinColumn(name = "cmps_orientation_id")
	private CmpsOrientation cmpsOrientation;
	
	
}
