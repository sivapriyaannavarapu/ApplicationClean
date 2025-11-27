package com.application.entity;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name="sce_stud_addrs" , schema = "sce_student")
public class StudentAddress {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int stud_addrs_id;
    private String house_no;
    private String street;
    private String landmark;
    private String area;
    @Column(name = "postal_code")
    private int postalCode;
    private int created_by;
    
    // CORRECTED: All are relationships to master tables
    @ManyToOne
    @JoinColumn(name = "city_id")
    private City city;

    @ManyToOne
    @JoinColumn(name = "mandal_id")
    private Mandal mandal;

    @ManyToOne
    @JoinColumn(name = "district_id")
    private District district;
    
    @ManyToOne
    @JoinColumn(name = "state_id")
    private State state; // State can also be a relationship

    @ManyToOne
    @JoinColumn(name = "stud_adms_id")
    private StudentAcademicDetails studentAcademicDetails;
}