package com.application.dto;

import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class StudentApplicationSingleDTO {

    // ===========================
    // 1. ACADEMIC DETAILS
    // ===========================
    private Integer studAdmsId;
    private Long studAdmsNo;
    private String firstName;
    private String lastName;
    private String apaarNo;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date appSaleDate;
    private String hallTicketNo;
    private Integer scoreMarks;
    
    // Lookups (ID & Name flattened)
    private Integer academicYearId;
    private String academicYearName;
    
    private Integer classId;
    private String className;
    
    private Integer branchId;
    private String branchName; // Campus
    
    private Integer admissionReferredByID; // Employee
    private String admissionReferredByName;
    
    private Integer quotaId;
    private String quotaName;
    
    private Integer genderId;
    private String genderName;
    
    private Integer admissionTypeId;
    private String admissionTypeName;
    
    private Integer studentTypeId;
    private String studentTypeName;
    
    private Integer studyTypeId;
    private String studyTypeName;
    
    private Integer cityId;
    private String cityName;

    // Previous School
    private String preSchoolName;
    private Integer preSchoolStateId;
    private String preSchoolStateName;
    private Integer preSchoolDistrictId;
    private String preSchoolDistrictName;

    // ===========================
    // 2. PERSONAL DETAILS
    // ===========================
    private Long aadharNo;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date dob;
    
    private Integer casteId;
    private String casteName;
    
    private Integer religionId;
    private String religionName;
    
    private Integer bloodGroupId;
    private String bloodGroupName;
    
    private Integer foodTypeId;
    private String foodTypeName;

    // ===========================
    // 3. ORIENTATION
    // ===========================
    private Integer orientationId;
    private String orientationName;
    
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date orientationStartDate;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date orientationEndDate;
    private Float orientationFee;
    // ===========================
    // 4. PARENT DETAILS
    // ===========================
    // Father
    private String fatherName;
    private Long fatherMobile;
    private String fatherEmail;
    private Integer fatherSectorId;
    private String fatherSectorName;
    private Integer fatherOccupationId;
    private String fatherOccupationName;

    // Mother
    private String motherName;
    private Long motherMobile;
    private String motherEmail;
    private Integer motherSectorId;
    private String motherSectorName;
    private Integer motherOccupationId;
    private String motherOccupationName;

    // ===========================
    // 5. ADDRESS DETAILS
    // ===========================
    private String doorNo;
    private String street;
    private String landmark;
    private String area;
    private Integer pincode;
    
    private Integer addressStateId;
    private String addressStateName;
    
    private Integer addressDistrictId;
    private String addressDistrictName;
    
    private Integer addressMandalId;
    private String addressMandalName;
    
    private Integer addressCityId;
    private String addressCityName;

    private List<SiblingItem> siblings = new ArrayList<>();
    private List<ConcessionItem> concessions = new ArrayList<>();

    @Data
    public static class SiblingItem {
        private String fullName;
        private String schoolName;
        private Integer relationId;
        private String relationName;
        private Integer classId;
        private String className;
    }

    @Data
    public static class ConcessionItem {
        private Integer concessionTypeId;
        private String concessionTypeName;
        private Float amount;
        private Integer reasonId;
        private String reasonName;
        private String comments;
        private Integer givenById;
        private String givenByName;      // NEW

        private Integer authorizedById;
        private String authorizedByName; // NEW

        private Integer concReferedBy;
        private String concReferedByName; // NEW
    }
}