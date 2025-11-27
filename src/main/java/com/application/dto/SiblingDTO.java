
package com.application.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SiblingDTO {
    private String fullName;
    private String schoolName;
    private Integer classId;
    private Integer relationTypeId;
    private Integer genderId;
    private Integer createdBy;
}
