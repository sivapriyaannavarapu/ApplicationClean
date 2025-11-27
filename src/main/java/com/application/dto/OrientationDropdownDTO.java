package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Objects; // For equals/hashCode

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrientationDropdownDTO {
    private int id; // orientationId
    private String name; // orientationName

    // Add equals and hashCode based on ID only for distinct checking
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrientationDropdownDTO that = (OrientationDropdownDTO) o;
        return id == that.id; // Distinct based on ID
    }

    @Override
    public int hashCode() {
        return Objects.hash(id); // Hash based on ID
    }
}