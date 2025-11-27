package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RateResponseDTO {
    private String type;
    private String permissionKey;
    private RateSectionDTO dropRated;
    private RateSectionDTO topRated;
}

