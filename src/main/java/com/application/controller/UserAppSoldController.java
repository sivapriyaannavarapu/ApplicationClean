package com.application.controller;
 
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.application.dto.RateResponseDTO;
import com.application.service.UserAppSoldService;
 
@RestController
@RequestMapping("/api/performance")
public class UserAppSoldController {
 
	@Autowired
	private UserAppSoldService userAppSoldService;
	 
	 @GetMapping("/top_drop_rate")//used
	    public ResponseEntity<List<RateResponseDTO>> getAllRateData() {
	        return ResponseEntity.ok(userAppSoldService.getAllRateData());
	    }
	 
}
