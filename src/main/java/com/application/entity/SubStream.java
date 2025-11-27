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
@Table(name="sce_sub_stream" , schema = "sce_course")
public class SubStream {
	
	@Id
	private int sub_stream_id;
	private String sub_stream;
	
	@ManyToOne
	@JoinColumn(name = "stream_id")
	private Stream stream;
	
}
