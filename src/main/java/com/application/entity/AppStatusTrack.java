	package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
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
@Table(name = "sce_app_stats_trk" , schema = "sce_application")
public class AppStatusTrack {

    @Id
    @Column(name = "app_stats_trk_id")  
    private int appStatsTrkId;

    @Column(name = "total_app")
    private int totalApp;

    @Column(name = "app_sold")
    private int appSold;

    @Column(name = "app_confirmed")
    private int appConfirmed;

    @Column(name = "app_available")
    private int appAvailable;

    @Column(name = "app_unavailable")
    private int appUnavailable;

    @Column(name = "app_damaged")
    private int appDamaged;

    @Column(name = "app_issued")
    private int appIssued;

    @Column(name = "is_active")
    private int isActive;
    
    @ManyToOne
    @JoinColumn(name = "zone_id")
    private Zone zone;
    
    @ManyToOne
    @JoinColumn(name = "cmps_id")
    private Campus campus;

    @ManyToOne
    @JoinColumn(name = "app_issued_type_id", referencedColumnName = "app_issued_id")
    private AppIssuedType issuedByType;

    @ManyToOne
    @JoinColumn(name = "emp_id", referencedColumnName = "emp_id") 
    private Employee employee;

    @ManyToOne
    @JoinColumn(name = "acdc_year_id", referencedColumnName = "acdc_year_id")
    private AcademicYear academicYear;
}

