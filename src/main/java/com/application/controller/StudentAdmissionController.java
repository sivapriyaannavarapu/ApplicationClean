
package com.application.controller;
 
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.ApiResponse;
import com.application.dto.ApplicationDetailsDTO;
import com.application.dto.BankDetailsDTO;
import com.application.dto.CampusAndZoneDTO;
import com.application.dto.CampusDetailsDTO;
import com.application.dto.ClassDTO;
import com.application.dto.ConcessionTypeDTO;
import com.application.dto.GenericDropdownDTO;
import com.application.dto.OrientationDTO;
import com.application.dto.OrientationFeeAndDatesDTO;
import com.application.dto.OrientationResponseDTO;
import com.application.dto.PinCodeLocationDTO;
import com.application.dto.StudentAdmissionDTO;
import com.application.dto.StudentSaleDTO;
import com.application.entity.StudyType;
import com.application.service.StudentAdmissionService;

import jakarta.persistence.EntityNotFoundException;
 
@RestController
@RequestMapping("/api/student-admissions-sale")
//@CrossOrigin(origins = "*")
public class StudentAdmissionController {
 
    @Autowired
    private StudentAdmissionService studentAdmissionService;
 
    // --- Endpoint for Form Submission ---
    @PostMapping("/create")
    public ResponseEntity<String> createAdmissionForm(@RequestBody StudentAdmissionDTO formDto) {
        try {
            studentAdmissionService.createAdmissionAndSale(formDto);
            return ResponseEntity.ok("Admission form created successfully!");
        } catch (Exception e) {
            // Log the full stack trace for debugging purposes
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to save form: " + e.getMessage());
        }
    }
    
    @PostMapping("/create/sale/only")
    public ResponseEntity<String> createAdmission(@RequestBody StudentSaleDTO formDto) {
        try {
            studentAdmissionService.createAdmission(formDto);
            return ResponseEntity.ok("Admission form created successfully!");
        } catch (Exception e) {
            // Log the full stack trace for debugging purposes
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to save form: " + e.getMessage());
        }
    }
    
    @GetMapping("/{pinCode}")//used/n
    public PinCodeLocationDTO getLocationByPinCode(@PathVariable int pinCode) {
        return studentAdmissionService.getLocationByPinCode(pinCode);
    }
    
    @GetMapping("/city/branchtype/{campusId}")//used/n
    public CampusDetailsDTO getCampusDetails(@PathVariable int campusId) {
        return studentAdmissionService.getCampusDetails(campusId);
    }
    
 
    // --- Endpoints for Populating Dropdowns ---
    @GetMapping("/admission-types")//used/c
    public List<GenericDropdownDTO> getAdmissionTypes() {
        return studentAdmissionService.getAllAdmissionTypes();
    }
 
    @GetMapping("/student-types")//used/c
    public List<GenericDropdownDTO> getStudentTypes() {
        return studentAdmissionService.getAllStudentTypes();
    }
 
    @GetMapping("/genders")//used/c
    public List<GenericDropdownDTO> getGenders() {
        return studentAdmissionService.getAllGenders();
    }
 
    @GetMapping("/campuses")
    public List<GenericDropdownDTO> getCampuses() {
        return studentAdmissionService.getAllCampuses();
    }
   
    @GetMapping("/orientations/{campusId}")
    public List<OrientationResponseDTO> getOrientationsByCampus(@PathVariable int campusId) {
        return studentAdmissionService.getOrientationsByCampus(campusId);
    }
 
    @GetMapping("/districts/{stateId}")//used/c
    public List<GenericDropdownDTO> getDistrictsByState(@PathVariable int stateId) {
        return studentAdmissionService.getDistrictsByState(stateId);
    }
 
    @GetMapping("/mandals/{districtId}")//used/c
    public List<GenericDropdownDTO> getMandalsByDistrict(@PathVariable int districtId) {
        return studentAdmissionService.getMandalsByDistrict(districtId);
    }
 
    @GetMapping("/cities/{districtId}")//used/c
    public List<GenericDropdownDTO> getCitiesByDistrict(@PathVariable int districtId) {
        return studentAdmissionService.getCitiesByDistrict(districtId);
    }
    
    @GetMapping("/organizations")//used/c
    public List<GenericDropdownDTO> getAllOrganizations() {
        return studentAdmissionService.getAllOrganizations();
    }
    
    @GetMapping("/banks/{orgId}")//used/c
    public List<GenericDropdownDTO> getBanksByOrganization(@PathVariable int orgId) {
        return studentAdmissionService.getBanksByOrganization(orgId);
    }
   
    @GetMapping("/all/Studentclass")//used/c
    public List<GenericDropdownDTO> getAllStudentClass() {
        return studentAdmissionService.getAllStudentclass();
    }
     
    @GetMapping("/studtype{id}")
    public ResponseEntity<StudyType> getStudyTypeById(@PathVariable int id) {
        StudyType studyType = studentAdmissionService.getStudyTypeById(id);
 
        if (studyType != null) {
            return ResponseEntity.ok(studyType);
        } else {
            return ResponseEntity.notFound().build();
        }
    }
  
    @GetMapping("/branches/{orgId}/{bankId}")//used/c
    public List<GenericDropdownDTO> getBranchesByOrganizationAndBank(
            @PathVariable int orgId,
            @PathVariable int bankId) {
        return studentAdmissionService.getBranchesByOrganizationAndBank(orgId, bankId);
    }
    
    @GetMapping("/bank-details")
    public ResponseEntity<BankDetailsDTO> getBankDetails(
            @RequestParam int orgId,
            @RequestParam int bankId,
            @RequestParam int branchId) {
        try {
            BankDetailsDTO bankDetails = studentAdmissionService.getBankDetails(orgId, bankId, branchId);
            return ResponseEntity.ok(bankDetails);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }
    
    @GetMapping("/quotas")//used/c
    public List<GenericDropdownDTO> getAllQuotas() {
        return studentAdmissionService.getAllQuotas();
    }
    
    @GetMapping("/authorizedBy/all")//used/c
    public List<GenericDropdownDTO> getAllEmployees() {
        return studentAdmissionService.getAllEmployees();
    }
    @GetMapping("/concessionReson/all")//used/c
    public List<GenericDropdownDTO> getAllConcessionReasons() {
        return studentAdmissionService.getAllConcessionReasons();
    }
    @GetMapping("/BloodGroup/all")
    public List<GenericDropdownDTO> getAllBloodGroups() {
        return studentAdmissionService.getAllBloodGroups();
    }
    @GetMapping("/PaymentModes/all")
    public List<GenericDropdownDTO> getAllPaymentModes() {
        return studentAdmissionService.getAllPaymentModes();
    }
    
    @GetMapping("/orientations/by-class/{classId}/cmps/{cmpsId}")//used/
    public List<OrientationDTO> getDistinctActiveOrientations(
            @PathVariable int classId, 
            @PathVariable int cmpsId) {
                
        // Call the new service method
        return studentAdmissionService.getDistinctActiveOrientationsByClassIdAndCmpsId(classId, cmpsId);
    }
    
    @GetMapping("/classes/by-campus/{campusId}")//used/c
    public List<ClassDTO> getClasses(@PathVariable int campusId) {
        return studentAdmissionService.getClassesByCampusId(campusId);
    }
 
    
    @GetMapping("Type_of_school")//used/c
    public List<GenericDropdownDTO> getAllSchoolTypes() 
    {      
    	return studentAdmissionService.getAllSchoolTypes(); 
    }
    
    @PostMapping("/concessiontype_ids")//used
    public ResponseEntity<List<ConcessionTypeDTO>> getConcessionTypesByNames(
            @RequestBody List<String> concTypes) {

        List<ConcessionTypeDTO> result = studentAdmissionService.getConcessionTypesByNames(concTypes);
        return ResponseEntity.ok(result);
    }
    
    @GetMapping("/relation-types")
    public List<GenericDropdownDTO> getRelationTypes() {
        return studentAdmissionService.getAllStudentRelations();
    }
    
    @GetMapping("/study-typebycmpsId_and_classId")
    public ResponseEntity<List<GenericDropdownDTO>> getStudyTypes(
        @RequestParam("cmpsId") int cmpsId,
        @RequestParam("classId") int classId
    ) {
        List<GenericDropdownDTO> studyTypes = studentAdmissionService.getStudyTypesByCampusAndClass(cmpsId, classId);
        if (studyTypes.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(studyTypes, HttpStatus.OK);
    }
    
    @GetMapping("/orientationbycmpsId_and_classId_and_studyType")
    public ResponseEntity<List<GenericDropdownDTO>> getOrientations(
        @RequestParam("cmpsId") int cmpsId,
        @RequestParam("classId") int classId,
        @RequestParam("studyTypeId") int studyTypeId
    ) {
        List<GenericDropdownDTO> orientations = studentAdmissionService.getOrientationsByCampusClassAndStudyType(cmpsId, classId, studyTypeId);
        if (orientations.isEmpty()) {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity<>(orientations, HttpStatus.OK);
    }

    @GetMapping("/religions")//used/
    public List<GenericDropdownDTO> getReligions() {
        return studentAdmissionService.getAllReligions();
    }

    @GetMapping("/castes")//used/
    public List<GenericDropdownDTO> getCastes() {
        return studentAdmissionService.getAllCastes();
    }
    
    @GetMapping("/details/{studAdmsNo}")
    public ResponseEntity<ApiResponse<?>> getApplicationDetails(
            @PathVariable Long studAdmsNo) {
        try {
            // Call the service method we created
            ApplicationDetailsDTO details = studentAdmissionService.getApplicationDetailsByAdmissionNo(studAdmsNo);
            
            // Return 200 OK with the fetched data
            return ResponseEntity.ok(ApiResponse.success(details, "Application details fetched successfully."));
            	
        } catch (EntityNotFoundException e) {
            // Return 404 Not Found if the student doesn't exist
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.error(e.getMessage()));
                    
        } catch (Exception e) {
            // Return 500 Internal Server Error for any other issues
            e.printStackTrace(); // Log the full error stack trace for debugging
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch application details: " + e.getMessage()));
        }
    }
    
    
    @PutMapping("/update_details/{studAdmsNo}")
    public ResponseEntity<ApiResponse<?>> updateApplicationDetails(
            @PathVariable Long studAdmsNo,
            @RequestBody StudentSaleDTO saleDTO) {
        try {
            // The DTO must include the studAdmsNo for the service layer
            saleDTO.setStudAdmsNo(studAdmsNo);
            
            // Call the service method to update the entities
            StudentSaleDTO updatedDetails = studentAdmissionService.updateApplicationDetails(saleDTO);
            
            // Return 200 OK with the updated data
            return ResponseEntity.ok(ApiResponse.success(updatedDetails, "Application details updated successfully."));
                
        } catch (EntityNotFoundException e) {
            // Return 404 Not Found if the student doesn't exist
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.error(e.getMessage()));
                    
        } catch (Exception e) {
            // Return 500 Internal Server Error for any other issues
            e.printStackTrace(); // Log the full error stack trace for debugging
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to update application details: " + e.getMessage()));
        }
    }
    
    @GetMapping("/by-application-no/{applicationNo}")
    public ResponseEntity<ApiResponse<CampusAndZoneDTO>> getDetailsWithFee(
            @RequestParam Long appNo) { // Assuming appNo is int
        try {
        	CampusAndZoneDTO details = studentAdmissionService.getApplicationDetailsWithFee(appNo);
            return ResponseEntity.ok(ApiResponse.success(details, "Details fetched successfully."));
        } catch (EntityNotFoundException e) {
            // Return 404 if any required linked record is missing
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.error(e.getMessage()));
        } catch (Exception e) {
            e.printStackTrace(); // Log error
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch application details: " + e.getMessage()));
        }
    }
    
    @GetMapping("/by-city/Campuses/{cityId}")
    public ResponseEntity<List<GenericDropdownDTO>> getCampuses(@PathVariable int cityId) {
        List<GenericDropdownDTO> result = studentAdmissionService.getCampusesByCityId(cityId);
        return ResponseEntity.ok(result); // Returns [] if empty
    }
    
 
    @GetMapping("/studet-type/{orientationId}/{campusId}")
    public ResponseEntity<String> getCampusType(
            @PathVariable Integer orientationId,
            @PathVariable Integer campusId) {
 
        String cmpsType = studentAdmissionService.getCampusType(orientationId, campusId);
 
        return ResponseEntity.ok(cmpsType != null ? cmpsType : "");
    }
    
 
    @GetMapping("/OrientationFeeDetails/{orientationId}")
    public ResponseEntity<OrientationFeeAndDatesDTO> getDetails(@PathVariable Integer orientationId) {
 
    	OrientationFeeAndDatesDTO dto = studentAdmissionService.getFeeAndDates(orientationId);
 
        return ResponseEntity.ok(dto);
    }
    
    @GetMapping("/{districtId}/{category}")
    public List<GenericDropdownDTO> getCategoryData(
            @PathVariable Integer districtId,
            @PathVariable String category) {
 
        return studentAdmissionService.getByCategoryAndDistrict(districtId, category);
    }

}
 