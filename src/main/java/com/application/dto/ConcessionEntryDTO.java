
package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConcessionEntryDTO {
    private Integer concTypeId; // e.g., 1 for "1st year", 2 for "2nd year"
    private Float amount;
}
