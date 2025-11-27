package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_app_issued_type", schema = "sce_application")
public class AppIssuedType {
    
    @Id
    @Column(name = "app_issued_id")
    private int appIssuedId;
    
    @Column(name = "type_name")
    private String typeName;

}
