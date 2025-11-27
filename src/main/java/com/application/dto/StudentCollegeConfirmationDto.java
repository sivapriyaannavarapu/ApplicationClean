package com.application.dto;

import java.sql.Date;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentCollegeConfirmationDto {
	
	    // Academic Details (Lookups)
	    private Integer academicYearId; 
	    private Integer joiningClassId;
	    private Integer branchId;
	    private Integer studentTypeId;
	    
	    // Location and Course Details
	    private Integer cityId;
	    private Integer courseNameId;
	    
	    // Assuming a way to link to the main student record
	    private Long studAdmsNo; 
	    private Integer createdBy;
	
	    private List<ConcessionConfirmationDTO> concessions;
	    private PaymentDetailsDTO paymentDetails;
}
