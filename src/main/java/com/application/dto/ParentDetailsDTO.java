
package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ParentDetailsDTO {
    private String name; // To hold father's or mother's name
    private Integer relationTypeId; // To identify Father, Mother, etc.
    private String occupation;
    private String otherOccupation; 
    private Long mobileNo;
    private String email;
    private Integer createdBy;
    private Integer sectorId;
}
