package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "acdc_year", schema = "sce_application")
public class AcademicYear {
    
    @Id
    @Column(name = "acdc_year_id")
    private int acdcYearId;
    
    @Column(name = "year")
    private int year;
    
    @Column(name = "acdc_year")
    private String academicYear;
    
}
