package com.application.dto;
 
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricCardDTO {
 
    private String title;
    private long value;
    private int percentage;
    private String state;
 
}