package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrientationBatchDetailsDTO {
    private Date orientationStartDate;
    private Date orientationEndDate;
    private float orientationFee;
}
