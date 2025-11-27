package com.application.dto;
 
import java.util.Date;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
 
/**
* Main DTO for the combined Student Admission (Step 1) and Payment (Step 2) forms.
*/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class StudentSaleDTO {
 
    // --- Personal Information (Step 1) ---
    private String firstName;
    private String lastName;
    private Integer genderId;
    private String apaarNo;
    private Date dob;
    private Long aadharCardNo;
    private Integer quotaId;      // From "Quota/Admission Referred By"
    private Long proReceiptNo;
    private Date appSaleDate;
    private String admissionReferedBy;
 
    // --- Parent Information (Step 1) ---
    private String fatherName;
    private Long fatherMobileNo;
 
    // --- Orientation Information (Step 1) ---
    private Integer academicYearId;
    private Integer branchId;       // Replaces campusId
    private Integer studentTypeId;
    private Integer classId;        // From "Joining Class"
    private Integer orientationId;  // From "Orientation Name"
    private Integer appTypeId;      // From "Admission Type"
 
    // --- Address Information (Step 1) ---
    private AddressDetailsDTO addressDetails;
    // --- Hidden/System Fields ---
    private Long studAdmsNo;    // This is your "Application No"
    private Integer createdBy;    // This 'createdBy' is for the Student record
}