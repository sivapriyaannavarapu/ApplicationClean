package com.application.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.DgmToCampusFormDTO;
import com.application.dto.DistributionRequestDTO;
import com.application.dto.FormSubmissionDTO;
import com.application.service.CampusService;
import com.application.service.DgmService;
import com.application.service.ZoneService;

@RestController
@RequestMapping("/distribution/posts")
//@CrossOrigin(origins = "*")
public class DistributionPosts {
	
	@Autowired
	private ZoneService distributionService;
	
	@Autowired
	private DgmService applicationService;
	 
	@Autowired
	private CampusService dgmService;
	
	@PostMapping("/zone-save")//used
	public ResponseEntity<String> saveDistribution(@RequestBody DistributionRequestDTO request) {
	    
	    // ADD THIS LINE FOR DEBUGGING
	    System.out.println("--- RECEIVED DATA IN CONTROLLER --- \n" + request.toString());

	    try {
	        distributionService.saveDistribution(request);
	        return ResponseEntity.ok("Distribution saved successfully!");
	    } catch (Exception e) {
	        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
	                             .body("Failed to save distribution: " + e.getMessage());
	    }
	}
	
	 @PostMapping("/dgm-save")//used
	    public ResponseEntity<String> submitForm(@RequestBody FormSubmissionDTO formDto) {
	        try {
	            applicationService.submitForm(formDto);
	            return ResponseEntity.ok("Form submitted successfully!");
	        } catch (Exception e) {
	            // Return the actual error message for easier debugging on the frontend
	            return ResponseEntity.internalServerError().body(e.getMessage());
	        }
	    }
	 
	 @PostMapping("/campus-save")//used
	    public ResponseEntity<String> submitForm(@RequestBody DgmToCampusFormDTO formDto) {
	        try {
	            dgmService.submitDgmToCampusForm(formDto);
	            return ResponseEntity.ok("DGM distribution saved successfully!");
	        } catch (Exception e) {
	            // Return the actual error message for easier debugging on the frontend
	            return ResponseEntity.internalServerError().body(e.getMessage());
	        }
	    }
}
