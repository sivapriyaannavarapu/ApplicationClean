package com.application.dto;
 
import lombok.Data;
import java.time.LocalDate;
 
@Data
public class DistributionRequestDTO {
    private int academicYearId;
    private int stateId;
    private int cityId;
    private int zoneId;
    private Float Application_Amount;
    
    private int issuedByTypeId;
    private int issuedToTypeId;
 
    private int issuedToEmpId;
    
    private int appStartNo;
    private int  appEndNo;
    private int range;
    
    private LocalDate issueDate;
    private int createdBy;
}