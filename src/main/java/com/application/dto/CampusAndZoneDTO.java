package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CampusAndZoneDTO {
	private Long applicationNo;
    private Integer campusId;
    private String campusName;
    private Integer zoneId;
    private String zoneName;
    private Integer academicYearId;
    private String academicYear;
    private Float applicationFee;
    private Float amount;
}
