package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_district", schema = "sce_locations")
public class District {

    @Id
    @Column(name = "district_id")
    private int districtId;

    @Column(name = "district_name")
    private String districtName;

    @ManyToOne
    @JoinColumn(name = "state_id", referencedColumnName = "state_id")
    private State state;
}
