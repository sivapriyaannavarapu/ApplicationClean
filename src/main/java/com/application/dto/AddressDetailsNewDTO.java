package com.application.dto;
 
import lombok.Data;

import lombok.NoArgsConstructor;

// Add AllArgsConstructor if needed
 
@Data

@NoArgsConstructor

public class AddressDetailsNewDTO {

    private String doorNo;

    private String street;

    private String landmark;

    private String area;

    private Integer pincode;

    private Integer districtId;

    private String districtName; // For display

    private Integer mandalId;

    private String mandalName; // For display

    private Integer cityId;

    private String cityName; // For display
    
    private Integer stateId;
    private String stateName;

    // We are skipping stateId/stateName as it wasn't listed for this screen

    // Skip Gpin for now

}
 