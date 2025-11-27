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
@Table(name = "sce_orientation_stream" , schema = "sce_course")
public class OrientationStream {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int orientation_stream_id;
	
	@ManyToOne
	@JoinColumn(name = "orienation_id")
	private Orientation orientation;
	
	@ManyToOne
	@JoinColumn(name = "stream_id")
	private Stream stream;
}
