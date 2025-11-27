package com.application.entity;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Immutable; // Mark as read-only

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Immutable // Views are typically read-only
@Table(name = "sce_parent_occupation", schema = "sce_student") // Assuming schema
public class ParentOccupationView {

    @EmbeddedId
    private ParentOccupationViewId id; // Composite ID

    // Map other columns (can be useful even if not directly used in dropdowns)
    @Column(name = "occupation_name")
    private String occupationName;

    @Column(name = "sector_name")
    private String sectorName;

    // Add isActive if the view has it, otherwise remove
    // @Column(name = "is_active")
    // private int isActive;
}