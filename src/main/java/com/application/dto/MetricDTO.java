package com.application.dto;
 
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricDTO {
    private String title;
    private long currentValue;
    private double percentageChange; // Percentage change from previous year
    private String changeDirection; // "up", "down", or "neutral"
}