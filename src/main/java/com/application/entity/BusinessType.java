package com.application.entity;
 
import jakarta.persistence.Column; // Ensure imported
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
 
@Data
@Entity
// --- Using correct table name, assuming default schema ---
@Table(name = "sce_business_type", schema = "sce_campus")
public class BusinessType {
 
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    // --- FIX: Map to the correct PK column name ---
    @Column(name = "id")
    private Integer businessTypeId; // Java name is fine
 
    // --- FIX: Map to the correct name column ---
    @Column(name = "business_name")
    private String businessTypeName; // Java name is fine
 
    // isActive field removed is correct
}