package com.application.controller;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.application.dto.ApiResponse;
import com.application.dto.BatchDTO;
import com.application.dto.CampusDropdownDTO;
import com.application.dto.OccupationSectorDropdownDTO;
import com.application.dto.OrientationBatchDetailsDTO;
import com.application.dto.OrientationDropdownDTO;
import com.application.dto.OrientationFeeDTO;
import com.application.dto.StudentConfirmationDTO;
import com.application.entity.BloodGroup;
import com.application.entity.ConcessionReason;
import com.application.entity.District;
import com.application.entity.FoodType;
import com.application.entity.Gender;
import com.application.entity.State;
import com.application.entity.StudentClass;
import com.application.entity.StudentRelation;
import com.application.entity.StudentType;
import com.application.service.ApplicationNewConfirmationService;

import jakarta.persistence.EntityNotFoundException;

@RestController
@RequestMapping("/api/application-confirmation")
//@CrossOrigin(origins = "*")
public class ApplicationConfirmationController {
    
    
    @Autowired ApplicationNewConfirmationService confirmationService;

    @GetMapping("/dropdown/relation/type")
    public ResponseEntity<ApiResponse<List<StudentRelation>>> getStudentRelations() {
        try {
            List<StudentRelation> relations = confirmationService.getActiveStudentRelations();
            return ResponseEntity.ok(
                ApiResponse.success(relations, "Relations fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch relations: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/classes")
    public ResponseEntity<ApiResponse<List<StudentClass>>> getClasses() {
        try {
            List<StudentClass> classes = confirmationService.getActiveClasses();
            return ResponseEntity.ok(
                ApiResponse.success(classes, "Classes fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch classes: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/genders")
    public ResponseEntity<ApiResponse<List<Gender>>> getGenders() {
        try {
            List<Gender> genders = confirmationService.getActiveGenders();
            return ResponseEntity.ok(
                ApiResponse.success(genders, "Genders fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch genders: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/batches")//used/c
    public ResponseEntity<ApiResponse<List<BatchDTO>>> getOrientationBatches(
            @RequestParam Integer orientationId) {
        try {
            if (orientationId == null) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("orientationId parameter is required."));
            }
            List<BatchDTO> batches = confirmationService.getBatchesByOrientation(orientationId);
            return ResponseEntity.ok(
                ApiResponse.success(batches, "Batches fetched successfully for orientation ID: " + orientationId)
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch batches: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/states")//used/c
    public ResponseEntity<ApiResponse<List<State>>> getStates() {
        try {
            List<State> states = confirmationService.getActiveStates();
            return ResponseEntity.ok(
                ApiResponse.success(states, "States fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch states: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/districts")
    public ResponseEntity<ApiResponse<List<District>>> getDistricts(
            @RequestParam Integer stateId) { // Use @RequestParam to get ID from URL
        try {
            if (stateId == null) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("stateId parameter is required."));
            }
            List<District> districts = confirmationService.getActiveDistrictsByState(stateId);
            return ResponseEntity.ok(
                ApiResponse.success(districts, "Districts fetched successfully for state ID: " + stateId)
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch districts: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/studytypes")
    public ResponseEntity<ApiResponse<List<StudentType>>> getStudyTypes() {
        try {
            List<StudentType> studyTypes = confirmationService.getActiveStudyTypes();
            return ResponseEntity.ok(
                ApiResponse.success(studyTypes, "Study types fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch study types: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/foodtypes")//used/c
    public ResponseEntity<ApiResponse<List<FoodType>>> getFoodTypes() {
        try {
            List<FoodType> studyTypes = confirmationService.getFoodTypes();
            return ResponseEntity.ok(
                ApiResponse.success(studyTypes, "Study types fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch study types: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/bloodGrouptypes")//used/c
    public ResponseEntity<ApiResponse<List<BloodGroup>>> getBloodGroupTypes() {
        try {
            List<BloodGroup> studyTypes = confirmationService.getBloodGroupTypes();
            return ResponseEntity.ok(
                ApiResponse.success(studyTypes, "Study types fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch study types: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/concessionReasontypes")
    public ResponseEntity<ApiResponse<List<ConcessionReason>>> getConcessionReasonTypes() {
        try {
            List<ConcessionReason> studyTypes = confirmationService.getConcessionReasonTypes();
            return ResponseEntity.ok(
                ApiResponse.success(studyTypes, "Study types fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch study types: " + e.getMessage()));
        }
    }
    
    @GetMapping("/orientation/batch/combo/details")//used/n
    public ResponseEntity<ApiResponse<OrientationBatchDetailsDTO>> getOrientationBatchComboDetails(
            @RequestParam Integer orientationId,
            @RequestParam Integer batchId) {
        try {
            if (orientationId == null || batchId == null) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("Both orientationId and batchId parameters are required."));
            }
            // Call the new service method
            Optional<OrientationBatchDetailsDTO> detailsOpt =
                confirmationService.getDetailsByOrientationAndBatch(orientationId, batchId);

            if (detailsOpt.isPresent()) {
                 return ResponseEntity.ok(
                    ApiResponse.success(detailsOpt.get(), "Details fetched successfully for orientation/batch combo.")
                 );
            } else {
                 return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(ApiResponse.error("Details not found for Orientation ID: " + orientationId + " and Batch ID: " + batchId));
            }
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch orientation/batch details: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/occupations")//used/c
    public ResponseEntity<ApiResponse<List<OccupationSectorDropdownDTO>>> getOccupations() {
        try {
            List<OccupationSectorDropdownDTO> occupations = confirmationService.getUniqueOccupations();
            return ResponseEntity.ok(
                ApiResponse.success(occupations, "Occupations fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch occupations: " + e.getMessage()));
        }
    }

    // --- UPDATE RESPONSE TYPE ---
    @GetMapping("/dropdown/sectors")//used/c
    public ResponseEntity<ApiResponse<List<OccupationSectorDropdownDTO>>> getSectors() {
        try {
            List<OccupationSectorDropdownDTO> sectors = confirmationService.getUniqueSectors();
            return ResponseEntity.ok(
                ApiResponse.success(sectors, "Sectors fetched successfully.")
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch sectors: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/orientations")//used/c
    public ResponseEntity<ApiResponse<List<OrientationDropdownDTO>>> getOrientations(
            @RequestParam Integer campusId, // Keep campusId
            @RequestParam Integer classId) {  // <-- ADD classId parameter
        try {
            // Check both parameters
            if (campusId == null || classId == null) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("Both campusId and classId parameters are required."));
            }
            // --- MODIFIED: Pass both IDs to service ---
            List<OrientationDropdownDTO> orientations =
                confirmationService.getOrientationsByCampusAndClass(campusId, classId);
 
            return ResponseEntity.ok(
                ApiResponse.success(orientations, "Orientations fetched successfully for campus ID: " + campusId + " and class ID: " + classId)
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch orientations: " + e.getMessage()));
        }
    }
    
    @GetMapping("/dropdown/campuses")//used/c
    public ResponseEntity<ApiResponse<List<CampusDropdownDTO>>> getCampusesByType(
            @RequestParam String businessType) { // Parameter name matching the frontend
        try {
            if (businessType == null || businessType.isBlank()) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("businessType parameter is required."));
            }
            List<CampusDropdownDTO> campuses = confirmationService.getCampusesByBusinessType(businessType.toUpperCase()); // Convert to uppercase to be safe
            return ResponseEntity.ok(
                ApiResponse.success(campuses, "Campuses fetched successfully for type: " + businessType)
            );
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch campuses: " + e.getMessage()));
        }
    }
                 
    
    @PostMapping("/confirm-school")
public ResponseEntity<?> saveStudentConfirmation(@RequestBody StudentConfirmationDTO dto) {
    try {
        confirmationService.saveOrUpdateConfirmation(dto);

        return ResponseEntity.ok("Student confirmation saved successfully.");

    } catch (EntityNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(e.getMessage());

    } catch (DataIntegrityViolationException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body("Database error: " + e.getMostSpecificCause().getMessage());

    } catch (Exception e) {
        e.printStackTrace();
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body("Unexpected error: " + e.getMessage());
    }
}

    
    
    @GetMapping("/orientation-fee")//used/n
    public ResponseEntity<ApiResponse<OrientationFeeDTO>> getOrientationFee(
            @RequestParam Integer orientationId) {
        try {
            if (orientationId == null) {
                 return ResponseEntity.badRequest()
                    .body(ApiResponse.error("orientationId parameter is required."));
            }
            // Call the new service method
            Optional<OrientationFeeDTO> feeOpt = confirmationService.getOrientationFeeById(orientationId);
 
            if (feeOpt.isPresent()) {
                 return ResponseEntity.ok(
                    ApiResponse.success(feeOpt.get(), "Orientation fee fetched successfully.")
                 );
            } else {
                 // Return success with null data if no fee found, or 404? Let's return success with null.
                 // This handles cases where orientation exists but might not have fee data in the view yet.
                  return ResponseEntity.ok(
                    ApiResponse.success(null, "No fee details found for Orientation ID: " + orientationId)
                  );
                 // Alternatively, return 404:
                 // return ResponseEntity.status(HttpStatus.NOT_FOUND)
                 //   .body(ApiResponse.error("Fee details not found for Orientation ID: " + orientationId));
            }
        } catch (Exception e) {
             e.printStackTrace(); // Log stack trace
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(ApiResponse.error("Failed to fetch orientation fee: " + e.getMessage()));
        }
    }
    
    
    

}