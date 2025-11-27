package com.application.dto;
public record GraphSoldSummaryDTO(
    long totalApplications,
    long totalSold
) {
    public GraphSoldSummaryDTO() {
        this(0L, 0L);
    }
}
 