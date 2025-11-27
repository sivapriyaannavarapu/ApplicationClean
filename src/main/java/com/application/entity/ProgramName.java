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
@Table(name = "sce_program_name", schema = "sce_course")
public class ProgramName {

	@Id
	@Column(name = "program_id")
	private int programId;
	@Column(name = "program_name")
	private String programName;
//	@Column(name = "stream_id")
//	private int streamId;
	private int class_id;
	private Integer promoting_program_id;
	private Integer no_hours_day;
	private Integer no_days_week;
	private Integer no_of_holidays;
	private Integer status;

//	@ManyToOne
//	@JoinColumn(name = "course_track_id")
//	private CourseTrack courseTrack;
}
