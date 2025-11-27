package com.application.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.DashboardResponseDTO;
import com.application.dto.GenericDropdownDTO;
import com.application.dto.MetricCardDTO;
import com.application.service.AppStatusTrackService;

@RestController
@RequestMapping("/api/dashboard/CO")
public class AppStatusTrackController {

	@Autowired
	private AppStatusTrackService appStatusTrackService;

	@GetMapping("/cards")
	public List<MetricCardDTO> getMetrics() {
	    return appStatusTrackService.getMetricCards();
	}
    
    @GetMapping("/dgm-employees")//used/c
    public List<GenericDropdownDTO> getAllDgmEmployees() {
        return appStatusTrackService.getAllDgmEmployees();
    }
    
    @GetMapping("/admin_cards_graph")
    public DashboardResponseDTO getDashboardOverview() {
        return appStatusTrackService.getDashboardData();
    }
}