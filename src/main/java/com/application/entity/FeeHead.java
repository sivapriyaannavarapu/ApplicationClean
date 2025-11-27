package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="sce_fee_head" , schema = "sce_course")
public class FeeHead {
	
	@Id
	private int fee_head_id;
	private String head_type;
	
}
