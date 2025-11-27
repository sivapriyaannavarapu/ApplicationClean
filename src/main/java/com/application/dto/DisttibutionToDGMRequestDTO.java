
package com.application.dto;
 
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
 
@Data
public class DisttibutionToDGMRequestDTO {
 
    @NotNull(message = "Academic Year ID is required")
    private Integer academicYearId;
 
    @NotNull(message = "City ID is required")
    private Integer cityId;
    
    @NotNull(message = "Zone ID is required")
    private Integer zoneId;
 
    @NotNull(message = "Campus ID is required")
    private Integer campusId;
 
    @NotNull(message = "Issued To Employee ID is required")
    private Integer issuedToEmpId;
 
    @NotEmpty(message = "Application 'From' number is required")
    private String applicationNoFrom;
 
    @NotNull(message = "Range is required")
    @Min(value = 1, message = "Range must be at least 1")
    private Integer range;
}
 
 
 