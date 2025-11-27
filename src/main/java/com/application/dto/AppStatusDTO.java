package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppStatusDTO {
    private int applicationNo;
    private String displayStatus;
    private String campus;
    private String zone;
}
