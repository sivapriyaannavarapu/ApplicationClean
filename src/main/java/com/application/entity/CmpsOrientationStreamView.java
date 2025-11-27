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
@Table(name = "sce_cmps_orientation_stream", schema = "sce_course")
public class CmpsOrientationStreamView {

    @Id
    @Column(name = "stream_name")
    private String streamName;
    @Column(name = "cmps_id")
    private int cmpsId;

    @Column(name = "cmps_name")
    private String cmpsName;

    @Column(name = "orientation_id") 
    private int orientationId;

    @Column(name = "orientation_name")
    private String orientationName;

    @Column(name = "stream_id")
    private int streamId;

}
