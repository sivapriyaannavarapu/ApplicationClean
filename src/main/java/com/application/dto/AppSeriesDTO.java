package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppSeriesDTO {
    private String displaySeries; // e.g., "2810001 - 2810050"
    private int startNo;
    private int endNo;
}