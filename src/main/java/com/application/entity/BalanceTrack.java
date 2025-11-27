package com.application.entity;

import jakarta.annotation.Generated;
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
@Table(name="sce_app_balance_trk" , schema = "sce_application") 
public class BalanceTrack {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "app_balance_trk_id")
    private int appBalanceTrkId;

    @Column(name = "app_avbl_cnt")
    private int appAvblCnt;

    @Column(name = "app_from")
    private int appFrom;

    @Column(name = "app_to")
    private int appTo;

    @Column(name = "is_active")
    private int isActive;

    @Column(name = "created_by")
    private int createdBy;

    @ManyToOne
    @JoinColumn(name = "acdc_year_id", referencedColumnName = "acdc_year_id")
    private AcademicYear academicYear;

    @ManyToOne
    @JoinColumn(name = "emp_id")
    private Employee employee;

    @ManyToOne
    @JoinColumn(name = "issued_type_id", referencedColumnName = "app_issued_id")
    private AppIssuedType issuedByType;
    
    private Float amount;	
    @Column(name = "issued_to_pro_id")
    private Integer issuedToProId;
}

