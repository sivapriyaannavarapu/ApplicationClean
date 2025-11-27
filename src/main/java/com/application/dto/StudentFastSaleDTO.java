package com.application.dto; // Assuming your DTO package; adjust if needed

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentFastSaleDTO {
    // Essential for lookup (not visible in form, but required)
	@JsonProperty("studAdmsNo")
    private Long studAdmsNo;
    private Integer createdBy;
    
    private Long aadharCardNo;
    private String apaarNo;

    // Personal Information (from image)
    private String firstName;
    private String lastName;
    private Integer genderId; // From Male/Female select
    private Date dob; // DD/MM/YYYY string
    private Integer appTypeId;
    // Admission Referred By
    private Integer quotaId; // From Quota select
    private Date app_sale_date;
    private String admissionReferredBy;

    // Parent Information
    private String fatherName;
    private Long fatherMobileNo; // Phone Number
//    private Integer proId; 
    // Orientation Information
    private Integer academicYearId; // AY 2025-2026 select (or fixed)
//    private Integer cityId; // Select City (may be for filtering, not saved directly)
    private Integer branchId; // Select Branch (Campus)
    private Integer classId; // Joining Class select
    private Integer orientationId; // Orientation Name select
    private Integer studentTypeId; // Student Type select
    
    private AddressDetailsDTO addressDetails;

    private PaymentDetailsDTO paymentDetails; 
}