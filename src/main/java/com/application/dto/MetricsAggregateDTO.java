package com.application.dto;

public record MetricsAggregateDTO(
    long totalApp,
    long appSold,
    long appConfirmed,
    long appAvailable,
    long appUnavailable,
    long appDamaged,
    long appIssued
) {

    public MetricsAggregateDTO() {
        this(0, 0, 0, 0, 0, 0, 0);
    }
}