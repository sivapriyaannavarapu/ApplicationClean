package com.application.dto;
 
import java.util.Date;
 
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrientationFeeAndDatesDTO {
    private Date startDate;
    private Date endDate;
    private float fee;
}