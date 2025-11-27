package com.application.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_state", schema = "sce_locations")
public class State {

    @Id
    @Column(name = "state_id")
    private int stateId;

    @Column(name = "state_name")
    private String stateName;
    
    @Column(name = "status")
    private int status;
    
    @ManyToOne
    @JoinColumn(name = "country_id", referencedColumnName = "country_id")
    private Country country;
}
