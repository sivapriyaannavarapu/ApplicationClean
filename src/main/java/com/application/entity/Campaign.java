package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_campaign", schema = "sce_locations")
public class Campaign {

    @Id
    @Column(name = "campaign_id")
    private int campaignId;

    @Column(name = "area_name")
    private String areaName;
    
    @ManyToOne
    @JoinColumn(name = "cmps_id") 
    private Campus campus;   

    @ManyToOne
    @JoinColumn(name = "district_id") 
    private District district;

    @ManyToOne
    @JoinColumn(name = "city_id")  
    private City city;

    @ManyToOne
    @JoinColumn(name = "zone_id")  
    private Zone zone;
}
