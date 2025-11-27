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
@Table(name = "sce_food_type" , schema = "sce_student")
public class FoodType {
	
	@Id
	private int food_type_id;
	private String food_type;
//	private int is_active;
}
