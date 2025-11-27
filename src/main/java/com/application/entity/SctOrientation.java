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
@Table(name = "sce_sct_orientation" , schema = "sce_course")
public class SctOrientation {
	
	@Id
	private int sct_orientation_id;
	private int sct_incharge_id;
	
	@ManyToOne
	@JoinColumn(name = "section_id")
	private Section section;
	
	@ManyToOne
	@JoinColumn(name = "cmps_orientation_id")
	private CmpsOrientation cmpsOrientation;
}
