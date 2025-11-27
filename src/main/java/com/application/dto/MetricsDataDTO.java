package com.application.dto;
 
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;
 
@Data
@NoArgsConstructor
public class MetricsDataDTO {
    private String title = "Current Year Metrics (vs. Previous Year)";
    private int currentYear;
    private int previousYear;
    private List<MetricDTO> metrics;
}