package com.application.controller;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.application.dto.AppSeriesDTO;
import com.application.dto.EmployeesDto;
import com.application.dto.GenericDropdownDTO;
import com.application.dto.LocationAutoFillDTO;
import com.application.entity.AcademicYear;
import com.application.entity.BalanceTrack;
import com.application.entity.City;
import com.application.entity.District;
import com.application.entity.State;
import com.application.entity.Zone;
import com.application.repository.BalanceTrackRepository;
import com.application.repository.EmployeeRepository;
import com.application.repository.SchoolDetailsRepository;
import com.application.service.CampusService;
import com.application.service.DgmService;
import com.application.service.ZoneService;

@RestController
@RequestMapping("/distribution/gets")
//@CrossOrigin(origins = "*")
public class DistributionGet {

    @Autowired
	EmployeeRepository employeeRepository;
	
	@Autowired
	CampusService campusService;
	
	@Autowired
	private ZoneService distributionService;
	
	@Autowired private BalanceTrackRepository balanceTrackRepository;
	
	DistributionGet(SchoolDetailsRepository schoolDetailsRepository) {
    }
	
	@GetMapping("/academic-years")//used/c
	public ResponseEntity<List<AcademicYear>> getAcademicYears() {
		return ResponseEntity.ok(distributionService.getAllAcademicYears());
	}

	@GetMapping("/states")//used/c
	public ResponseEntity<List<State>> getStates() {
		return ResponseEntity.ok(distributionService.getAllStates());
	}

	@GetMapping("/city/{stateId}")//used/c
	public ResponseEntity<List<City>> getCitiesByState(@PathVariable int stateId) {
		return ResponseEntity.ok(distributionService.getCitiesByState(stateId));
	}

	@GetMapping("/zones/{cityId}")//used/c
	public ResponseEntity<List<Zone>> getZonesByCity(@PathVariable int cityId) {
		return ResponseEntity.ok(distributionService.getZonesByCity(cityId));
	}
	
	 @GetMapping("/{empId}/mobile")
	    public String getMobileByEmpId(@PathVariable int empId) {
	        return employeeRepository.findMobileNoByEmpId(empId);
	    }
	 
	   // GET /api/zonal-accountants/zone/1/employees
	    @GetMapping("/zone/{zoneId}/employees")//used/
	    public List<EmployeesDto> getEmployeesByZone(@PathVariable int zoneId) {
	        return distributionService.getEmployeesByZone(zoneId);
	    }
	 
	    @Autowired
	    private DgmService applicationService;
	 
	    @GetMapping("/cities")//used/c
	    public List<GenericDropdownDTO> getCities() {
	        return applicationService.getAllCities();
	    }
	 
	    @GetMapping("/campus/{zoneId}")//used/c
	    public List<GenericDropdownDTO> getCampusesByZone(@PathVariable int zoneId) {
	        return applicationService.getCampusesByZoneId(zoneId);
	    }
	    
	    @GetMapping("/campusesforzonal_accountant/{empId}")//used/c
	    public List<GenericDropdownDTO> getCampusesByEmployee(@PathVariable int empId) {
	        return applicationService.getCampusesByEmployeeId(empId);
	    }
	    
	    @GetMapping("/campusesfordgm/{empId}")//used
	    public List<GenericDropdownDTO> getActiveCampusesByEmpId(@PathVariable Integer empId) {
	        return applicationService.getActiveCampusesByEmpId(empId);
	    }
	    
	    @GetMapping("/dgmforzonal_accountant/{empId}")//used
	    public List<GenericDropdownDTO> getActiveCampusesByEmployee(@PathVariable int empId) {
	        return applicationService.getActiveCampusesByEmployeeId(empId);
	    }
	    
	    @GetMapping("/issued-to")//used
	    public List<GenericDropdownDTO> getIssuedToTypes() {
	        return applicationService.getAllIssuedToTypes();
	    }
	    
	    @GetMapping("/mobile-no/{empId}")//used/n
	    public ResponseEntity<String> getMobileNo(@PathVariable int empId) {
	        String mobileNumber = applicationService.getMobileNumberByEmpId(empId);
	        if (mobileNumber != null) {
	            return ResponseEntity.ok(mobileNumber);
	        } else {
	            return ResponseEntity.notFound().build();
	        }
	    }
	    
	    @Autowired
	    private CampusService dgmService;
	   
	  
	    @GetMapping("/districts/{stateId}")//used
	    public List<GenericDropdownDTO> getDistrictsByState(@PathVariable int stateId) {
	        return dgmService.getDistrictsByStateId(stateId);
	    }
	 
	    @GetMapping("/cities/{districtId}")//used/
	    public List<GenericDropdownDTO> getCitiesByDistrict(@PathVariable int districtId) {
	        return dgmService.getCitiesByDistrictId(districtId);
	    }
	    
	    @GetMapping("/campuses/{cityId}")//used/
	    public List<GenericDropdownDTO> getCampusesByCity(@PathVariable int cityId) {
	        return dgmService.getCampusesByCityId(cityId);
	    }
	    
	    @GetMapping("/campaign-areas")//used
	    public List<GenericDropdownDTO> getAllCampaignAreas() {
	        return dgmService.getAllCampaignAreas();
	    }
	     
	    @GetMapping("/pros/{campusId}")//used/c
	    public ResponseEntity<List<GenericDropdownDTO>> getEmployeeDropdown(
	            @PathVariable int campusId) {

	        // 1. Call the service layer method to execute the JPQL query
	        List<GenericDropdownDTO> employees = dgmService.getEmployeeDropdownByCampus(campusId);

	        // 2. Return the list with an HTTP 200 OK status
	        if (employees.isEmpty()) {
	            // Optional: Return 204 No Content or an empty list if no results
	            return ResponseEntity.ok(employees);
	        }
	        return ResponseEntity.ok(employees);
	    }
	    
	    @GetMapping("/getalldistricts")//used/
	    public List<District> getAllDistricts()
	    {
	    	return campusService.getAllDistricts();
	    }
	    
	    @GetMapping("/{campaignId}/campus")
	    public ResponseEntity<List<GenericDropdownDTO>> getCampusByCampaign(@PathVariable int campaignId) {
	        List<GenericDropdownDTO> campus = campusService.getCampusByCampaignId(campaignId);
	        return ResponseEntity.ok(campus);
	    }
	    
	    @GetMapping("/getarea/{cityId}")
	    public ResponseEntity<List<GenericDropdownDTO>> getCampaignsByCity(@PathVariable int cityId) {
	        List<GenericDropdownDTO> campaigns = campusService.getCampaignsByCityId(cityId);
	        return ResponseEntity.ok(campaigns);
	    }
	    
	    @GetMapping("/dgm/{campusId}")//used/c
	    public ResponseEntity<List<GenericDropdownDTO>> getActiveDgmEmployeesByCampus(
	            @PathVariable int campusId) { // Use @PathVariable

	        // Call the service method to fetch the data
	        List<GenericDropdownDTO> dgmEmployees = applicationService.getDgmEmployeesForCampus(campusId);

	        if (dgmEmployees.isEmpty()) {
	            // Return 404 Not Found if no employees are found
	            return ResponseEntity.notFound().build();
	        }

	        // Return the list with HTTP 200 OK
	        return ResponseEntity.ok(dgmEmployees);
	    }
	    
	    @GetMapping("/getallamounts/{empId}/{academicYearId}") // UPDATED PATH
	    public ResponseEntity<List<Double>> getFeeDropdown(
	        @PathVariable int empId,
	        @PathVariable int academicYearId // NEW PATH VARIABLE
	    ) {
	        List<Double> fees = applicationService.getApplicationFees(empId, academicYearId);
	        return ResponseEntity.ok(fees);
	    }
	    
	    @GetMapping("/district_city_autopopulate/{empId}/{category}")
	    public ResponseEntity<LocationAutoFillDTO> autoFill(
	            @PathVariable int empId,
	            @PathVariable String category) {

	        LocationAutoFillDTO dto = applicationService.getAutoPopulateData(empId, category);

	        return ResponseEntity.ok(dto);
	    }
	    
	    @GetMapping("/track")
	     public ResponseEntity<BalanceTrack> getBalanceTrackByEmployeeAndYear(
	             @RequestParam int academicYearId,
	             @RequestParam int employeeId) {

	         Optional<BalanceTrack> balanceTrackOptional = balanceTrackRepository.findBalanceTrack(academicYearId, employeeId);

	         if (balanceTrackOptional.isPresent()) {
	             return ResponseEntity.ok(balanceTrackOptional.get());
	         } else {
	             return ResponseEntity.notFound().build();
	         }
	     }
	     
	     @GetMapping("/get-series")
	     public ResponseEntity<List<AppSeriesDTO>> getSeriesDropdown(
	             @RequestParam int receiverId, 
	             @RequestParam Double amount,
	             @RequestParam boolean isPro) { // Frontend passes true if receiver is PRO
	             
	         return ResponseEntity.ok(applicationService.getActiveSeriesForReceiver(receiverId, amount, isPro));
	     }

	     // 2. Get the Distribution ID (Call this when user selects a Series)
	     @GetMapping("/get-distribution-id")
	     public ResponseEntity<Integer> getDistributionId(
	             @RequestParam int receiverId,
	             @RequestParam int start,
	             @RequestParam int end,
	             @RequestParam Double amount,
	             @RequestParam boolean isPro) {
	             
	         return ResponseEntity.ok(applicationService.getDistributionIdBySeries(receiverId, start, end, amount, isPro));
	     }
	 	
}
