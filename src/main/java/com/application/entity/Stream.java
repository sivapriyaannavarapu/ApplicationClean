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
@Table(name = "sce_stream" , schema = "sce_course")
public class Stream {
	
	@Id
	@Column(name = "stream_id")
	private int streamId;
	@Column(name = "stream_name")
    private String streamName; 
	private String stream_colour;
	private int test_stream;
	private int onsite_reg;
	
//	
//	@ManyToOne
//	@JoinColumn(name = "course_track_id")
//	private CourseTrack courseTrack;
}
