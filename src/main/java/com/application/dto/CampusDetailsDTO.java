package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CampusDetailsDTO {
    private String campusType;
    private int cityId;
    private String cityName;
}
