package com.application.dto;

import lombok.Data;

@Data
public class FormSubmissionDTO {
    private int userId; // The Zone Officer (Issuer)
    private int academicYearId;
    private int cityId;
    private int zoneId;
    private int campusId; // Context only
//    private int issuedToId;
    
    private int dgmEmployeeId; // The Receiver (DGM)
    
    // --- ADD THIS ---
    private Float application_Amount; 
    // ----------------

    private String applicationNoFrom;
    private String applicationNoTo;
    private int range;
    
    // Remove selectedBalanceTrackId if you want to clean up, or just ignore it
}