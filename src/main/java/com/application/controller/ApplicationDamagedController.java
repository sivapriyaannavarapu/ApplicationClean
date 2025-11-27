package com.application.controller;
 
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.AppStatusDetailsDTO;
import com.application.dto.AppStatusResponseDTO;
import com.application.dto.ApplicationDamagedDto;
import com.application.dto.Appstatusdtodamaged;
import com.application.dto.CampusDto;
import com.application.dto.EmployeeDto;
import com.application.dto.GenericDropdownDTO;
import com.application.entity.AppStatusTrackView;
import com.application.entity.ApplicationStatus;
import com.application.entity.Campus;
import com.application.service.ApplicationDamagedService;
 
@RestController
@RequestMapping("/api/applications")
public class ApplicationDamagedController {
 
    @Autowired
    private ApplicationDamagedService applicationDamagedService;
    @GetMapping("/pro-employees/{campusId}")
    public ResponseEntity<List<EmployeeDto>> getEmployeeNamesByCampusId(@PathVariable int campusId) {
        List<EmployeeDto> employees = applicationDamagedService.getEmployeeNamesByCampusId(campusId);
        if (employees.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(employees, HttpStatus.OK);
    }
    
    @GetMapping("/campuses-by-dgm-id")
    public ResponseEntity<List<Campus>> getCampusesByDgmId(@RequestParam int dgmId) {
        // Call the new service method that finds campuses by DGM ID
        List<Campus> campuses = applicationDamagedService.getCampusesByDgmId(dgmId);

        // If no campuses are found, you might want to return a 404 response
        if (campuses.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(campuses);
    }
 
    @GetMapping("/zones")//need/c
    public ResponseEntity<List<GenericDropdownDTO>> getAllZones() {
        List<GenericDropdownDTO> zone = applicationDamagedService.getAllZones();
        return new ResponseEntity<>(zone, HttpStatus.OK);
    }
 
    @GetMapping("/by-dgm/{zoneId}")
    public ResponseEntity<List<EmployeeDto>> getDgmNamesByZoneId(@PathVariable int zoneId) {
        List<EmployeeDto> dgmEmployees = applicationDamagedService.getDgmNamesByZoneId(zoneId);
        if (dgmEmployees.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(dgmEmployees);
    }
 
    @GetMapping("/campuses")//used/c
public ResponseEntity<List<GenericDropdownDTO>> getActiveCampusesForDropdown() {
        
        // 1. Call the service layer to get the filtered DTO list
        List<GenericDropdownDTO> activeCampuses = applicationDamagedService.getActiveCampusesDropdown();
        
        // 2. Return the list with an OK (200) status code
        if (activeCampuses.isEmpty()) {
            // Return 204 No Content if the list is empty, or 200 OK with an empty list
            return ResponseEntity.ok(activeCampuses); 
        }
        
        return ResponseEntity.ok(activeCampuses);
    }
    
    @GetMapping("/dgmcampuses")//used/
    public ResponseEntity<List<GenericDropdownDTO>> getDgmCampusesForDropdown() {
        
        // 1. Call the service layer to execute the JPQL query
        List<GenericDropdownDTO> campuses = applicationDamagedService.getDgmCampusesDropdown();
        
        // 2. Return a 200 OK response with the list of DTOs
        if (campuses.isEmpty()) {
            // Optional: Return 204 No Content if the list is empty
            return ResponseEntity.noContent().build();
        }
        
        return ResponseEntity.ok(campuses);
    }
 
    @PostMapping("/status")//need
    public ResponseEntity<?> createApplicationStatus(@RequestBody ApplicationDamagedDto requestDTO) {
        try {
            AppStatusResponseDTO response = applicationDamagedService.saveOrUpdateApplicationStatus(requestDTO);
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
 
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        }
    }
 
 
    @GetMapping("/statuses")//need
    public ResponseEntity<List<ApplicationStatus>> getAllStatuses() {
        List<ApplicationStatus> statuses = applicationDamagedService.getAllStatus();
        return new ResponseEntity<>(statuses, HttpStatus.OK);
    }
 
    @GetMapping("/{applicationNo}")//need
    public ResponseEntity<?> getApplicationDetails(@PathVariable Integer applicationNo) {

        Appstatusdtodamaged dto =
                applicationDamagedService.getDetailsByApplicationNo(applicationNo);

        if (dto == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("Application not found with number: " + applicationNo);
        }

        return ResponseEntity.ok(dto);
    }    
    @GetMapping("/details/{appNo}")
    public ResponseEntity<AppStatusDetailsDTO> getAppDetails(
            @PathVariable int appNo) {
        
        return applicationDamagedService.getAppStatusDetails(appNo)
                .map(ResponseEntity::ok)        // If the DTO is present, return 200 OK
                .orElse(ResponseEntity.notFound().build()); // If not found, return 404 Not Found
    }
    

    @GetMapping("/by-zone/{zoneId}")
    public ResponseEntity<List<CampusDto>> getCampusesByZone(@PathVariable int zoneId) {
        
        // Correct method name to match the one in your service class
        List<CampusDto> campuses = applicationDamagedService.getCampusDtosByZoneId(zoneId);

        return ResponseEntity.ok(campuses);
    }
    
    @GetMapping("/by_campus/damaged_details")
    public AppStatusTrackView getAppStatusByCampusAndNumber(
            @RequestParam int appNo,
            @RequestParam String campusName) {
        return applicationDamagedService.getAppStatusByCampusAndNumber(appNo, campusName);
    }
    
}