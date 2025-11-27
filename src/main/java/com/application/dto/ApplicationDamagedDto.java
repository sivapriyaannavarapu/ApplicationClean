package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationDamagedDto {

    private Integer applicationNo;
    private Integer statusId;
    private String reason;
    private Integer campusId;
    private Integer proId;
    private Integer zoneId;
    private Integer dgmEmpId;
}