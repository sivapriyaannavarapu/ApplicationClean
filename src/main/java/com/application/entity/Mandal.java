package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table; // Import Table
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_mandal", schema = "sce_locations") // Assuming schema and table name
public class Mandal {
    
    @Id
    private int mandal_id;
    private String mandal_name;
    
    // The relationship to District is the only reference you need.
    @ManyToOne
    @JoinColumn(name = "district_id")
    private District district;
    
    @ManyToOne
    @JoinColumn(name = "state_id")
    private State state;
}