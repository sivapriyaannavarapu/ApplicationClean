package com.application.dto;
 
import lombok.Data;
 
@Data
public class AddressDetailsDTO {
    private String doorNo;
    private String street;
    private String landmark;
    private String area;
    private Integer cityId; 
    private Integer mandalId; 
    private Integer districtId;
    private Integer pincode;
    private Integer stateId;
    private int createdBy;   
}