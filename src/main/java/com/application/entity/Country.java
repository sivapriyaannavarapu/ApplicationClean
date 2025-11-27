package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_country", schema = "sce_locations")
public class Country {

    @Id
    @Column(name = "country_id")
    private int countryId;

    @Column(name = "country_name")
    private String countryName;

}
