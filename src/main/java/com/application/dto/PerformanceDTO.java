package com.application.dto;
 
import lombok.Data;
 
@Data
public class PerformanceDTO {
    // A generic field to hold the name of the Zone, DGM, or Campus
    private String name;
    private double performancePercentage;
}