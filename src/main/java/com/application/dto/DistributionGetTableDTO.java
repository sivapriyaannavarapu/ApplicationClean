package com.application.dto;
 
import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DistributionGetTableDTO {
 
    private int appDistributionId;
    private int appStartNo;
    private int appEndNo;
    private int totalAppCount;
    private float amount;
    private int isActive;
    private int created_by;
    private int issued_to_emp_id;
    private int issued_by_type_id;
    private int issued_to_type_id;
    private int city_id;
    private int state_id;
    private int zone_id;
    private int district_id;
    private int cmps_id;
    private LocalDateTime issueDate;
    private int acdc_year_id;
    private int campaignAreaId;
 
    // Derived fields
    private String issuedToName;
    private String zoneName;
    private String campusName;
    private String dgmName;
    private String campaignAreaName;
 
}