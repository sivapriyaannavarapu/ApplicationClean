package com.application.dto;

import lombok.Data;
import java.time.LocalDate;

@Data
public class DgmToCampusFormDTO {
    // --- Context ---
    private int userId;              // Issuer
    private int academicYearId;
    private int cityId;
    private int campaignDistrictId;
    
    // --- Receiver ---
    private int branchId;            // Location Context
    private int receiverId;          // Specific Person/Agent
    
    // --- RESTORED FIELDS ---
    private int issuedToTypeId;      // Coming from Frontend
    private LocalDate issueDate;     // Coming from Frontend
    // -----------------------

    // --- Range & Amount ---
    private Float application_Amount; 
    private String applicationNoFrom; 
    private String applicationNoTo;   
    private int range;               
    
    private String category; // "School" or "College"
}