package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LocationAutoFillDTO {
    private Integer cityId;
    private String cityName;
    private Integer districtId;
    private String districtName;
}
