package com.application.dto;
 
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
 
public class EmployeeDto {
	private Integer empId;
    private String name;
}