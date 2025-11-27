package com.application.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DashboardResponseDTO {
    private List<MetricCardDTO> metricCards;  // For the cards section
    private GraphResponseDTO graphData;       // For the issued/sold graph
}
