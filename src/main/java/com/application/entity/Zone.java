package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_zone", schema = "sce_locations")
public class Zone {

    @Id
    @Column(name = "zone_id")
    private int zoneId;

    @Column(name = "zone_name")
    private String zoneName;
    

    @ManyToOne
    @JoinColumn(name = "city_id", referencedColumnName = "city_id")
    private City city;
}
