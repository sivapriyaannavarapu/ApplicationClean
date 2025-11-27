package com.application.entity;
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
@Table(name = "sce_user_app_sold" ,schema="sce_application")
public class UserAppSold {
 
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long userAppSold;
    private Integer empId;
    private Integer entityId;
    private Integer acdcYearId;
    private Long rangeStartNo;
    private Long rangeEndNo;
    private Integer totalAppCount;
    private Integer sold;
    private Float amount;


    @ManyToOne
    @JoinColumn(name = "zone_id")
    private Zone zone;
    
    @ManyToOne
    @JoinColumn(name = "cmps_id")
    private Campus campus;
    
    @Column(name = "is_active")
    private Integer isActive;
    
   
}