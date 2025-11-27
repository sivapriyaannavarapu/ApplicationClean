// CampusDto.java
package com.application.dto;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CampusDto {
    private int campusId;
    private String campusName;
}