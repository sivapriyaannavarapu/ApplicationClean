package com.application.service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SequenceGeneratorService {

    @PersistenceContext
    private final EntityManager entityManager;

    /**
     * Fetches the next available ID from the sce_app_distribution_id_seq sequence.
     * @return The next unique integer ID for a Distribution.
     */
    public int getNextDistributionId() {
        // --- THIS QUERY IS NOW SCHEMA-QUALIFIED ---
        // It now looks inside the 'sce_application' schema for the sequence.
        Query query = entityManager.createNativeQuery("SELECT nextval('sce_application.sce_app_distribution_id_seq')");
        return ((Number) query.getSingleResult()).intValue();
    }

    /**
     * Fetches the next available ID from the sce_app_balance_trk_id_seq sequence.
     * @return The next unique integer ID for a BalanceTrack.
     */
    public int getNextBalanceTrackId() {
        // --- THIS QUERY IS NOW SCHEMA-QUALIFIED ---
        Query query = entityManager.createNativeQuery("SELECT nextval('sce_application.sce_app_balance_trk_id_seq')");
        return ((Number) query.getSingleResult()).intValue();
    }
}