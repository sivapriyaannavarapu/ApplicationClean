package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class GraphBarDTO {

    private String year;

    // percentages
    private int issuedPercent;
    private int soldPercent;

    // actual values
    private int issuedCount;
    private int soldCount;
}
