package com.application.dto;
 
import java.util.List;

import lombok.Data;
 
@Data
public class GraphDTO {
 
    private String title = "Application Sales Percentage (2022-2025)";
    // Updated to use the new DTO version
    private List<YearlyGraphPointDTO> yearlyData;
}