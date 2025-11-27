package com.application.dto;

import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Appstatusdtodamaged {
	
	private int applicationNo;

    private Integer proEmpId;
    private String proName;

    private Integer dgmEmpId;
    private String dgmName;

    private Integer zoneId;
    private String zoneName;

    private Integer campusId;
    private String campusName;

    private String status;
    private LocalDateTime statusDate;
}
