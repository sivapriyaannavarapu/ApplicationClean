package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EmployeesDto {
    private int empId;         
    private String firstName;  
    private String lastName;  
    private String primaryMobileNo;   
}