package com.application.entity; // Or a sub-package like com.application.entity.ids

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.io.Serializable;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
public class ParentOccupationViewId implements Serializable {

    @Column(name = "occupation_id")
    private int occupationId;

    @Column(name = "occupation_sector_id")
    private int occupationSectorId;

    // IMPORTANT: equals() and hashCode() are required for composite keys
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ParentOccupationViewId that = (ParentOccupationViewId) o;
        return occupationId == that.occupationId &&
               occupationSectorId == that.occupationSectorId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(occupationId, occupationSectorId);
    }
}