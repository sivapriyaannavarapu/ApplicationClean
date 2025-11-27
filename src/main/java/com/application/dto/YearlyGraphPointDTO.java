package com.application.dto;
 
import lombok.Data;
 
/**
* DTO for the graph, built from UserAppSold table.
* The bars will represent Total Applications (100%) and Sold Applications (percentage of total).
*/
@Data
public class YearlyGraphPointDTO {
    private String year; // e.g., "2023-24"
    private double totalAppPercent; // This will always be 100
    private double soldPercent;
    private long totalAppCount;
    private long soldCount;
 
    public YearlyGraphPointDTO(String year, double totalAppPercent, double soldPercent, long totalAppCount, long soldCount) {
        this.year = year;
        this.totalAppPercent = totalAppPercent;
        this.soldPercent = soldPercent;
        this.totalAppCount = totalAppCount;
        this.soldCount = soldCount;
    }
 
    public YearlyGraphPointDTO() {
        this.year = "";
        this.totalAppPercent = 0.0;
        this.soldPercent = 0.0;
        this.totalAppCount = 0L;
        this.soldCount = 0L;
    }   
}