package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppRangeDTO {
    private Integer appStartNo;
    private Integer appEndNo;
    private Integer appFrom;
    private Integer appBalanceTrkId;
}
