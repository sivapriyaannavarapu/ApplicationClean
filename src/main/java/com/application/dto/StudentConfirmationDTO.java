package com.application.dto;

import java.util.Date;
import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This is the main DTO for the entire Student Confirmation form.
 */
@Data
@NoArgsConstructor
public class StudentConfirmationDTO {

    // --- Student Info ---
    private Long studAdmsNo; // The Admission No we are confirming
    
    private Integer createdBy; // The user performing the confirmation
    private Date appConfDate;  // The confirmation date
    
    // --- Personal Info ---
    private Integer foodTypeId;
    private Integer bloodGroupId;
    private Integer casteId;
    private Integer religionId;

    // --- Academic Info ---
    private String htNo; // Hallticket Number
    private Integer orientationId;
    private Integer orientationBatchId;
    private Date orientationDate;
//    private Float orientationFee;
    private Integer schoolStateId;
    private Integer schoolDistrictId;
    private Integer schoolTypeId;
    private String schoolName;
    private String scoreAppNo;
    private Integer marks;
    
    // --- Parent Info (List) ---
    // Uses your existing ParentDetailsDTO
    private List<ParentDetailsDTO> parents;

    // --- Sibling Info (List) ---
    // Uses your existing SiblingDTO
    private List<SiblingDTO> siblings;
    
    private List<LanguageDTO> languages; 


    // --- Concession Info (List) ---
    private List<ConcessionConfirmationDTO> concessions;
    
    // --- Payment Details ---
    private PaymentDetailsDTO paymentDetails; 
}