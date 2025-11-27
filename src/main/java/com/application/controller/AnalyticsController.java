package com.application.controller;
 
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.CombinedAnalyticsDTO;
import com.application.dto.GraphDTO;
import com.application.service.ApplicationAnalyticsService;
 
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {
 
    @Autowired
    private ApplicationAnalyticsService analyticsService;

    @GetMapping("/zone/{id}")
    public ResponseEntity<CombinedAnalyticsDTO> getZoneAnalytics(@PathVariable Long id) {
        try {
            CombinedAnalyticsDTO data = analyticsService.getZoneAnalytics(id);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            System.err.println("Error in getZoneAnalytics: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }
 
    @GetMapping("/campus/{id}")
    public ResponseEntity<CombinedAnalyticsDTO> getCampusAnalytics(@PathVariable Long id) {
        try {
            CombinedAnalyticsDTO data = analyticsService.getCampusAnalytics(id);
            return ResponseEntity.ok(data);
        } catch (Exception e) {
            System.err.println("Error in getCampusAnalytics: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.internalServerError().build();
        }
    }
    
    @GetMapping("/{empId}")
    public ResponseEntity<CombinedAnalyticsDTO> getRollupAnalytics(
            @PathVariable("empId") Integer empId) {
        
        try {
            // Call the "master rollup" method in the service
            CombinedAnalyticsDTO analytics = analyticsService.getRollupAnalytics(empId);
 
            // Check if the service returned empty data (e.g., role is PRO)
            if (analytics.getGraphData() == null && analytics.getMetricsData() == null) {
                // This returns the JSON { "role": "PRO", "entityName": "This role does not have a rollup view", ... }
                return ResponseEntity.badRequest().body(analytics);
            }
            return ResponseEntity.ok(analytics);
            
        } catch (Exception e) {
            System.err.println("Error in getRollupAnalytics: " + e.getMessage());
            e.printStackTrace();
            return ResponseEntity.internalServerError().body(null);
        }
    }
    
    @GetMapping("/zone/graph-by-amount")
    public ResponseEntity<GraphDTO> getZoneGraphByAmount(
            @RequestParam("zoneId") Integer zoneId,
            @RequestParam("amount") Float amount) {

        GraphDTO graphData = analyticsService.getGraphDataByZoneIdAndAmount(zoneId, amount);
        return ResponseEntity.ok(graphData);
    }
    
    @GetMapping("/campus/graph-by-amount")
    public ResponseEntity<GraphDTO> getCampusGraphByAmount(
            @RequestParam("campusId") Integer campusId,
            @RequestParam("amount") Float amount) {

        GraphDTO graphData = analyticsService.getGraphDataByCampusIdAndAmount(campusId, amount);
        return ResponseEntity.ok(graphData);
    }
}