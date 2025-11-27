package com.application.dto;
 
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
public class CombinedAnalyticsDTO {
    private String role;
    private String designationName;
    private String entityName;
    private Integer entityId;
    private GraphDTO graphData;
    private MetricsDataDTO metricsData;
    
}