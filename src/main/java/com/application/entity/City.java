package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_city", schema = "sce_locations")
public class City {
    
    @Id
    @Column(name = "city_id")
    private int cityId;
    
    @Column(name = "city_name")
    private String cityName;

    @ManyToOne
    @JoinColumn(name = "district_id")
    private District district;
    
    private int status;
}
