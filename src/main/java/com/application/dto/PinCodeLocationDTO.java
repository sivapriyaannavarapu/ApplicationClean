package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class PinCodeLocationDTO {
    private int stateId;
    private String stateName;
    private int districtId;
    private String districtName;
}
