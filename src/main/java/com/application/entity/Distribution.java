package com.application.entity;
 
import java.time.LocalDateTime;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="sce_app_distrubution", schema = "sce_application")
public class Distribution {
 
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "app_distrubution_id")
    private Integer appDistributionId;
 
    @Column(name = "app_start_no")
    private int appStartNo;
 
    @Column(name = "app_end_no")
    private int appEndNo;
 
    @Column(name = "total_app_count")
    private int totalAppCount;
 
    private Float amount;
 
    @Column(name = "is_active")
    private int isActive;
 
    private int created_by; // logged employee id
    private Integer issued_to_emp_id;
    private Integer issued_to_pro_id;
 
 
    @ManyToOne
    @JoinColumn(name = "created_by", referencedColumnName = "emp_id", insertable = false, updatable = false)
    private Employee createdByEmployee;
    
    @ManyToOne
    @JoinColumn(name = "issued_to_emp_id", referencedColumnName = "emp_id", insertable = false, updatable = false)
    private Employee issuedToEmployee;
 
    @ManyToOne
    @JoinColumn(name = "issued_by_type_id", referencedColumnName = "app_issued_id")
    private AppIssuedType issuedByType;
 
    @ManyToOne
    @JoinColumn(name = "issued_to_type_id", referencedColumnName = "app_issued_id")
    private AppIssuedType issuedToType;
 
    @ManyToOne
    @JoinColumn(name = "city_id")
    private City city;
 
    @ManyToOne
    @JoinColumn(name = "state_id")
    private State state;
 
    @ManyToOne
    @JoinColumn(name = "zone_id")
    private Zone zone;
 
    @ManyToOne
    @JoinColumn(name = "district_id")
    private District district;
 
    @ManyToOne
    @JoinColumn(name = "cmps_id")
    private Campus campus;
    
    @Column(name = "created_date")
    private LocalDateTime issueDate;
    
    @ManyToOne
    @JoinColumn(name = "acdc_year_id")
    private AcademicYear academicYear;
}
 
 