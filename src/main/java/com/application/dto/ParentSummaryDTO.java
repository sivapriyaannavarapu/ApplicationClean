package com.application.dto;
 
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParentSummaryDTO {
    private String fatherName;
    private Long phoneNumber;
    // Add occupation if needed, though not listed for this screen
}