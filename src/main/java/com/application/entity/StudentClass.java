package com.application.entity;

import jakarta.persistence.*;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_class", schema = "sce_student")
public class StudentClass {

    @Id
    @Column(name = "class_id")
    private int classId;

    @Column(name = "class_name")
    private String className;
    
    @Column(name = "is_active") 
    private int isActive;
}
