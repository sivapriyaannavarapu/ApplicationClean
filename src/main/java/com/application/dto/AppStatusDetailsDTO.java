// AppStatusDetailsDTO.java
package com.application.dto;

import lombok.Data;

@Data
public class AppStatusDetailsDTO {
    private int proId;
    private String proName;
    private int zoneId;
    private String zoneName;
    private int dgmEmpId;
    private String dgmEmpName;
    private int campusId;
    private String campusName;
    private String status;
    private String reason;
}