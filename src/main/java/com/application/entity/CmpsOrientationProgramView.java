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
@Table(name = "sce_cmps_orientation_program", schema = "sce_course")
public class CmpsOrientationProgramView {

    @Id
    @Column(name = "cmps_name")
    private String cmpsName;
    @Column(name = "cmps_id")
    private int cmpsId;

    @Column(name = "orientation_id")
    private int orientationId;

    @Column(name = "orientation_name")
    private String orientationName;

    @Column(name = "program_id")
    private int programId;

    @Column(name = "program_name")
    private String programName;

    @Column(name = "exam_program_id")
    private int examProgramId;

    @Column(name = "exam_program_name")
    private String examProgramName;
}
