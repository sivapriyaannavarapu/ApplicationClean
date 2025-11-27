package com.application.dto;
 
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppNumberRangeDTO {
    private int id;
    private int appFrom;
    private int appTo;
}
 