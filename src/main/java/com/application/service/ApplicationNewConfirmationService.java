package com.application.service;

import java.time.LocalDateTime; // For created_date fix
import java.util.Comparator;
// ---
// --- Add these imports ---
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
//import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.application.dto.BatchDTO;
import com.application.dto.CampusDropdownDTO;
// --- Import all your DTOs ---
import com.application.dto.ConcessionConfirmationDTO;
import com.application.dto.LanguageDTO;
import com.application.dto.OccupationSectorDropdownDTO;
import com.application.dto.OrientationBatchDetailsDTO;
import com.application.dto.OrientationDropdownDTO;
import com.application.dto.OrientationFeeDTO;
import com.application.dto.ParentDetailsDTO;
import com.application.dto.PaymentDetailsDTO;
import com.application.dto.SiblingDTO;
import com.application.dto.StudentConfirmationDTO;
import com.application.entity.AcademicYear;
import com.application.entity.BloodGroup;
import com.application.entity.CmpsOrientationBatchFeeView;
import com.application.entity.ConcessionReason;
import com.application.entity.District;
import com.application.entity.FoodType;
import com.application.entity.Gender;
import com.application.entity.Orientation;
import com.application.entity.ParentDetails;
import com.application.entity.ParentOccupationView;
import com.application.entity.PaymentDetails;
import com.application.entity.Sibling;
import com.application.entity.State;
import com.application.entity.Status;
// --- Import all your Entities ---
import com.application.entity.StudentAcademicDetails;
import com.application.entity.StudentApplicationTransaction;
import com.application.entity.StudentClass;
import com.application.entity.StudentConcessionType;
import com.application.entity.StudentOrientationDetails;
import com.application.entity.StudentPersonalDetails;
import com.application.entity.StudentRelation;
import com.application.entity.StudentType;
import com.application.repository.AcademicYearRepository;
import com.application.repository.BloodGroupRepository;
import com.application.repository.CampusRepository;
import com.application.repository.CampusSchoolTypeRepository;
import com.application.repository.CasteRepository;
import com.application.repository.CityRepository;
import com.application.repository.CmpsOrientationBatchFeeViewRepository;
import com.application.repository.ConcessionReasonRepository;
import com.application.repository.ConcessionTypeRepository;
import com.application.repository.DistrictRepository;
import com.application.repository.FoodTypeRepository;
import com.application.repository.GenderRepository;
import com.application.repository.OrgBankBranchRepository;
import com.application.repository.OrgBankRepository;
import com.application.repository.OrientationBatchRepository;
import com.application.repository.OrientationRepository;
import com.application.repository.ParentDetailsRepository;
import com.application.repository.ParentOccupationViewRepository;
import com.application.repository.PaymentDetailsRepository;
import com.application.repository.PaymentModeRepository;
import com.application.repository.ReligionRepository;
import com.application.repository.SectorRepository;
import com.application.repository.SiblingRepository;
import com.application.repository.StateRepository;
import com.application.repository.StatusRepository;
// --- Import all your Repositories ---
// ... (All your repositories) ...
import com.application.repository.StudentAcademicDetailsRepository;
import com.application.repository.StudentApplicationTransactionRepository;
import com.application.repository.StudentClassRepository;
import com.application.repository.StudentConcessionTypeRepository;
import com.application.repository.StudentOrientationDetailsRepository;
import com.application.repository.StudentPersonalDetailsRepository;
import com.application.repository.StudentRelationRepository;
import com.application.repository.StudentTypeRepository;

import jakarta.persistence.EntityNotFoundException;




@Service
public class ApplicationNewConfirmationService {

    // --- AUTOWIRE ALL NECESSARY REPOSITORIES ---
    // (Make sure all these are autowired at the top of your class)
    @Autowired private StudentAcademicDetailsRepository academicRepo;
    @Autowired private StudentPersonalDetailsRepository personalRepo;
    @Autowired private StudentOrientationDetailsRepository orientationRepo;
    @Autowired private ParentDetailsRepository parentRepo;
    @Autowired private SiblingRepository siblingRepo; 
    @Autowired private StudentConcessionTypeRepository concessionRepo;
    @Autowired private StatusRepository statusRepo;
    @Autowired private AcademicYearRepository academicYearRepo;
    @Autowired private FoodTypeRepository foodTypeRepo;
    @Autowired private BloodGroupRepository bloodGroupRepo; 
    @Autowired private OrientationBatchRepository orientationBatchRepo;
    @Autowired private StateRepository stateRepo;
    @Autowired private DistrictRepository districtRepo;
    @Autowired private CampusSchoolTypeRepository schoolTypeRepo;
    @Autowired private StudentRelationRepository relationRepo;
    @Autowired private StudentClassRepository classRepo;
    @Autowired private GenderRepository genderRepo;
    @Autowired private ConcessionTypeRepository concessionTypeRepo;
    @Autowired private ConcessionReasonRepository concessionReasonRepo;
    @Autowired 
    private StudentRelationRepository studentRelationRepo;
    @Autowired 
    private CmpsOrientationBatchFeeViewRepository cmpsOrientationBatchFeeViewRepo;
    @Autowired 
    private StudentTypeRepository studyTypeRepo;
    @Autowired 
    private ParentOccupationViewRepository parentOccupationViewRepo;
    @Autowired private OrientationRepository orientationMasterRepo;
    @Autowired private PaymentDetailsRepository paymentDetailsRepo; // --- ADD ---
    @Autowired private PaymentModeRepository paymentModeRepo;
    @Autowired private CampusRepository campusRepo;
    @Autowired private OrgBankRepository orgBankRepo;
    @Autowired private OrgBankBranchRepository orgBankBranchRepo;
    @Autowired private CityRepository cityRepo;
    @Autowired private StudentApplicationTransactionRepository studentApplicationTransactionRepo;
    @Autowired private CasteRepository casteRepository;
    @Autowired private ReligionRepository religionRepository;
    @Autowired private SectorRepository sectorRepository;
 
    
    
    
    @Cacheable(value = "studentRelations") 
    public List<StudentRelation> getActiveStudentRelations() {
        return studentRelationRepo.findByIsActive(1); 
    }
    
    @Cacheable(value = "studentClasses")
    public List<StudentClass> getActiveClasses() {
        return classRepo.findByIsActive(1); // Assuming 1 = Active
    }
    
    @Cacheable(value = "genders")
    public List<Gender> getActiveGenders() {
        return genderRepo.findByIsActive(1); // Assuming 1 = Active
    }
    
    @Cacheable(value = "batchesByOrientation", key = "#orientationId")
    public List<BatchDTO> getBatchesByOrientation(int orientationId) {

        // Call the NEW repository method that returns BatchDTO directly
        List<BatchDTO> distinctBatches =
            cmpsOrientationBatchFeeViewRepo.findDistinctBatchesByOrientationId(orientationId);

        return distinctBatches;
    }
    
    @Cacheable(value = "states")
    public List<State> getActiveStates() {
    	return stateRepo.findAll();
    }
    
    @Cacheable(value = "districtsByState", key = "#stateId")
    public List<District> getActiveDistrictsByState(int stateId) {
        return districtRepo.findByStateStateId(stateId); // Assuming 1 = Active
    }
    
    @Cacheable(value = "studyTypes")
    public List<StudentType> getActiveStudyTypes() {
        return studyTypeRepo.findAll(); // Assuming 1 = Active
    }
    @Cacheable(value = "foodTypes")
    public List<FoodType> getFoodTypes() {
        return foodTypeRepo.findAll(); // Assuming 1 = Active
    }
    
    @Cacheable(value = "BloodGroupTypes")
    public List<BloodGroup> getBloodGroupTypes() {
        return bloodGroupRepo.findAll(); // Assuming 1 = Active
    }
    
    @Cacheable(value = "BloodGroupTypes")
    public List<ConcessionReason> getConcessionReasonTypes() {
        return concessionReasonRepo.findAll(); // Assuming 1 = Active
    }
    
    @Cacheable(value = "campusesByBusinessType", key = "#businessTypeName")
    public List<CampusDropdownDTO> getCampusesByBusinessType(String businessTypeName) {
        return campusRepo.findActiveCampusesByBusinessTypeName(businessTypeName);
    }
    
//    @Cacheable(value = "orientationAndBatchDetails", key = "{#orientationId, #orientationBatchId}")
    public Optional<OrientationBatchDetailsDTO> getDetailsByOrientationAndBatch(int orientationId, int orientationBatchId) {
        // Use the new repository method
        List<OrientationBatchDetailsDTO> detailsList =
            cmpsOrientationBatchFeeViewRepo.findDetailsByOrientationAndBatchId(orientationId, orientationBatchId);

        // Return the first element if the list is not empty
        if (detailsList != null && !detailsList.isEmpty()) {
            return Optional.of(detailsList.get(0));
        } else {
            return Optional.empty(); // No details found for this combination
        }
    }
    
    @Cacheable(value = "occupations")
    public List<OccupationSectorDropdownDTO> getUniqueOccupations() {
        // 1. Fetch all rows (or active rows)
        List<ParentOccupationView> allOccupations = parentOccupationViewRepo.findAll(); // Or findByIsActive(1)

        // 2. Group by occupation name and select one ID (e.g., the minimum) for each name
        Map<String, Integer> uniqueOccupationMap = allOccupations.stream()
                .filter(o -> o.getOccupationName() != null && !o.getOccupationName().isBlank()) // Ensure name is not null/blank
                .collect(Collectors.toMap(
                        ParentOccupationView::getOccupationName,          // Key is the name
                        view -> view.getId().getOccupationId(),          // Value is the ID
                        (existingId, newId) -> existingId                // If duplicate name, keep the first ID found
                ));

        // 3. Convert the map to a list of DTOs
        return uniqueOccupationMap.entrySet().stream()
                .map(entry -> new OccupationSectorDropdownDTO(entry.getValue(), entry.getKey()))
                // 4. Sort alphabetically by name
                .sorted(Comparator.comparing(OccupationSectorDropdownDTO::getName))
                .collect(Collectors.toList());
    }

    /**
     * Gets unique sectors for the parent dropdown, ensuring unique names.
     */
    @Cacheable(value = "sectors")
    public List<OccupationSectorDropdownDTO> getUniqueSectors() {
        // 1. Fetch all rows (or active rows)
        List<ParentOccupationView> allSectors = parentOccupationViewRepo.findAll(); // Or findByIsActive(1)

        // 2. Group by sector name and select one ID for each name
        Map<String, Integer> uniqueSectorMap = allSectors.stream()
                .filter(o -> o.getSectorName() != null && !o.getSectorName().isBlank()) // Ensure name is not null/blank
                .collect(Collectors.toMap(
                        ParentOccupationView::getSectorName,               // Key is the name
                        view -> view.getId().getOccupationSectorId(),     // Value is the ID
                        (existingId, newId) -> existingId                 // If duplicate name, keep the first ID found
                ));

        // 3. Convert the map to a list of DTOs
        return uniqueSectorMap.entrySet().stream()
                .map(entry -> new OccupationSectorDropdownDTO(entry.getValue(), entry.getKey()))
                // 4. Sort alphabetically by name
                .sorted(Comparator.comparing(OccupationSectorDropdownDTO::getName))
                .collect(Collectors.toList());
    }
    
    @Cacheable(value = "orientationsByCampusAndClass", key = "{#campusId, #classId}")
    public List<OrientationDropdownDTO> getOrientationsByCampusAndClass(int campusId, int classId) { // <-- Change parameters
        // --- MODIFIED: Call the new repository method ---
        return cmpsOrientationBatchFeeViewRepo.findDistinctOrientationsByCampusAndClass(campusId, classId);
    }
    
//    @Cacheable(value = "orientationFee", key = "#orientationId")
    public Optional<OrientationFeeDTO> getOrientationFeeById(int orientationId) {
        // Use the existing repository method
        List<CmpsOrientationBatchFeeView> detailsList =
            cmpsOrientationBatchFeeViewRepo.findByOrientationId(orientationId);
 
        // Find the first non-null result and extract the fee
        return detailsList.stream()
                .filter(Objects::nonNull) // Ensure the view object itself isn't null
                .findFirst() // Get the first record found
                .map(view -> new OrientationFeeDTO(view.getOrientationFee())); // Create DTO from the fee
    }
    
    @Transactional
    public StudentAcademicDetails saveOrUpdateConfirmation(StudentConfirmationDTO dto) {
        
        // --- 1. Find the Student ---
        StudentAcademicDetails student = academicRepo.findByStudAdmsNo(dto.getStudAdmsNo())
            .orElseThrow(() -> new EntityNotFoundException("Student not found with Admission No: " + dto.getStudAdmsNo()));
 
        // --- 2. Update Academic Details ---
        student.setHt_no(dto.getHtNo());
        student.setApp_conf_date(dto.getAppConfDate());
        student.setPre_school_name(dto.getSchoolName());
        student.setScore_app_no(dto.getScoreAppNo());
        student.setIs_active(1);
        
        if(dto.getMarks() != null) {
            student.setScore_marks(dto.getMarks());
        }
        
        Status activeStatus = statusRepo.findById(1)
            .orElseThrow(() -> new EntityNotFoundException("Status 'Active' (ID: 1) not found"));
        student.setStatus(activeStatus);
 
        if (dto.getSchoolStateId() != null) {
            stateRepo.findById(dto.getSchoolStateId()).ifPresent(student::setState);
        }
        if (dto.getSchoolDistrictId() != null) {
            districtRepo.findById(dto.getSchoolDistrictId()).ifPresent(student::setDistrict);
        }
        if (dto.getSchoolTypeId() != null) {
            schoolTypeRepo.findById(dto.getSchoolTypeId()).ifPresent(student::setPreCampusSchoolType);
        }
        
        if (dto.getLanguages() != null && !dto.getLanguages().isEmpty()) {

            int[] languageIds = dto.getLanguages().stream().mapToInt(LanguageDTO::getLangId).toArray();

            student.setLang_id(languageIds);

        }
        academicRepo.save(student);
 
        // --- 3. Update Personal Details ---
        StudentPersonalDetails personalDetails = personalRepo.findByStudentAcademicDetails(student)
            .orElse(new StudentPersonalDetails());
        personalDetails.setStudentAcademicDetails(student);
        
        if (dto.getFoodTypeId() != null) {
            foodTypeRepo.findById(dto.getFoodTypeId()).ifPresent(personalDetails::setFoodType);
        }
        if (dto.getBloodGroupId() != null) {
            bloodGroupRepo.findById(dto.getBloodGroupId()).ifPresent(personalDetails::setBloodGroup);
        }
        if(dto.getCasteId() !=null) {
        	casteRepository.findById(dto.getCasteId()).ifPresent(personalDetails::setCaste);
        }
        if(dto.getReligionId() !=null) {
        	religionRepository.findById(dto.getReligionId()).ifPresent(personalDetails::setReligion);
        }
        personalRepo.save(personalDetails);
 
        // --- 4. Update Orientation Details ---
        StudentOrientationDetails orientationDetails = orientationRepo.findByStudentAcademicDetails(student)
            .orElse(new StudentOrientationDetails());
        orientationDetails.setStudentAcademicDetails(student);
        
        orientationDetails.setOrientation_date(dto.getOrientationDate());
//        student.setAdditional_orientation_fee(dto.getOrientationFee() != null ? dto.getOrientationFee().intValue() : 0);
        
        if (dto.getOrientationId() != null) {
            Orientation selectedOrientation = orientationMasterRepo.findById(dto.getOrientationId())
                .orElseThrow(() -> new EntityNotFoundException("Orientation not found for ID: " + dto.getOrientationId()));
            orientationDetails.setOrientation(selectedOrientation);
        }
        if (dto.getOrientationBatchId() != null) {
            orientationBatchRepo.findById(dto.getOrientationBatchId()).ifPresent(orientationDetails::setOrientationBatch);
        }
        orientationRepo.save(orientationDetails);
        
        // --- 5. Save/Update Parent Details (UPSERT LOGIC) ---
        if (dto.getParents() != null && !dto.getParents().isEmpty()) {
            
            // --- NO DELETE ---
            // parentRepo.deleteAll(parentRepo.findByStudentAcademicDetails(student));
            
            // 1. Fetch existing parents and put them in a Map for easy lookup
            // We use the RelationType ID as the unique key (e.g., 1=Father, 2=Mother)
        	Map<Integer, ParentDetails> existingParentsMap = parentRepo
        		    .findByStudentAcademicDetails(student)
        		    .stream()
        		    .filter(p -> p.getStudentRelation() != null)  // also fix getter name here
        		    .collect(Collectors.toMap(
        		        p -> p.getStudentRelation().getRelationId(),  // Fixed
        		        Function.identity()
        		    ));
            
            
 
            for (ParentDetailsDTO parentDto : dto.getParents()) {
                
                // 2. Check if a parent with this relation type already exists
                ParentDetails parent = existingParentsMap.get(parentDto.getRelationTypeId());
                
                if (parent == null) {
                    // 3. IF NOT: Create a new ParentDetails object
                    parent = new ParentDetails();
                    parent.setStudentAcademicDetails(student);
                    parent.setCreated_by(parentDto.getCreatedBy());
                    if (parentDto.getRelationTypeId() != null) {
                        relationRepo.findById(parentDto.getRelationTypeId()).ifPresent(parent::setStudentRelation);
                    }
                }
                
                // 4. Update all fields (for both new and existing parents)
                parent.setName(parentDto.getName());
                parent.setMobileNo(parentDto.getMobileNo());
                parent.setEmail(parentDto.getEmail());
                if (parentDto.getSectorId() != null) {
                    // Assuming you have a SectorRepository injected as 'sectorRepo'
                    sectorRepository.findById(parentDto.getSectorId()).ifPresent(parent::setSector);
                }
                if (parentDto.getOccupation() != null) {
                    if ("OTHERS".equalsIgnoreCase(parentDto.getOccupation())) {
                        parent.setOccupation(parentDto.getOtherOccupation());  // store typed value
                    } else {
                        parent.setOccupation(parentDto.getOccupation()); // store dropdown value
                    }
                }
                // Note: We don't update created_by for an existing record
                
                parentRepo.save(parent);
            }
        }
 
        // --- 6. Save/Update Sibling Details (UPSERT LOGIC) ---
        // WARNING: This logic is not perfect. If you change a sibling's name in the
        // form, this code will create a NEW sibling instead of updating the old one.
        // This is the best we can do without DELETE permission.
        if (dto.getSiblings() != null && !dto.getSiblings().isEmpty()) {
            
            // --- NO DELETE ---
            // siblingRepo.deleteAll(siblingRepo.findByStudentAcademicDetails(student));
            
            // 1. Fetch existing siblings and put them in a Map by Full Name
            // This is a fragile key, but it's the only one we have.
            Map<String, Sibling> existingSiblingsMap = siblingRepo.findByStudentAcademicDetails(student)
                .stream()
                .filter(s -> s.getSibling_name() != null)
                .collect(Collectors.toMap(Sibling::getSibling_name, Function.identity(), (first, second) -> first)); // Handle duplicates
 
            for (SiblingDTO siblingDto : dto.getSiblings()) {
                
                // 2. Check if a sibling with this name already exists
                Sibling sibling = existingSiblingsMap.get(siblingDto.getFullName());
                
                if (sibling == null) {
                    // 3. IF NOT: Create a new Sibling
                    sibling = new Sibling();
                    sibling.setStudentAcademicDetails(student);
                    sibling.setCreated_by(siblingDto.getCreatedBy());
                    sibling.setSibling_name(siblingDto.getFullName());
                }
                
                // 4. Update all fields
                sibling.setSibling_school(siblingDto.getSchoolName());
                if (siblingDto.getRelationTypeId() != null) {
                    relationRepo.findById(siblingDto.getRelationTypeId()).ifPresent(sibling::setStudentRelation);
                }
                if (siblingDto.getClassId() != null) {
                    classRepo.findById(siblingDto.getClassId()).ifPresent(sibling::setStudentClass);
                }
                if (siblingDto.getGenderId() != null) {
                    genderRepo.findById(siblingDto.getGenderId()).ifPresent(sibling::setGender);
                }
                siblingRepo.save(sibling);
            }
        }
        
        // --- 7. Save/Update Concession Details (UPSERT LOGIC) ---
        if (dto.getConcessions() != null && !dto.getConcessions().isEmpty()) {
            
            // --- NO DELETE ---
            // concessionRepo.deleteAll(concessionRepo.findByStudAdmsId(student.getStud_adms_id()));
 
            // 1. Fetch existing concessions and put them in a Map
            // We use ConcessionType ID as the key.
            // WARNING: This assumes a student only has ONE of each concession type.
            Map<Integer, StudentConcessionType> existingConcessionsMap =
                concessionRepo.findByStudAdmsId(student.getStud_adms_id())
                .stream()
                .filter(c -> c.getConcessionType() != null)
                .collect(Collectors.toMap(
                    c -> c.getConcessionType().getConcTypeId(),
                    Function.identity(),
                    (first, second) -> first // Handle duplicates
                ));
 
            AcademicYear currentYear = academicYearRepo.findById(student.getAcademicYear().getAcdcYearId())
                .orElseThrow(() -> new EntityNotFoundException("Academic Year not found"));
 
            for (ConcessionConfirmationDTO concDto : dto.getConcessions()) {
                
                // 2. Check if a concession with this type already exists
                StudentConcessionType concession = existingConcessionsMap.get(concDto.getConcessionTypeId());
                
                if (concession == null) {
                    // 3. IF NOT: Create a new Concession
                    concession = new StudentConcessionType();
                    concession.setStudAdmsId(student.getStud_adms_id());
                    concession.setAcademicYear(currentYear);
                    concession.setCreated_by(concDto.getCreatedBy());
                    concession.setCreated_Date(LocalDateTime.now()); // Set create date
                    if (concDto.getConcessionTypeId() != null) {
                        concessionTypeRepo.findById(concDto.getConcessionTypeId()).ifPresent(concession::setConcessionType);
                    }
                }
 
                // 4. Update all fields
                concession.setConc_amount(concDto.getConcessionAmount());
                concession.setComments(concDto.getComments());
                
                if (concDto.getReasonId() != null) {
                    concessionReasonRepo.findById(concDto.getReasonId()).ifPresent(concession::setConcessionReason);
                }
                
                concession.setConc_referred_by(concDto.getConcReferedBy());
                
                if (concDto.getGivenById() != null) {
                    concession.setConc_issued_by(concDto.getGivenById());
                }
                if (concDto.getAuthorizedById() != null) {
                    concession.setConc_authorised_by(concDto.getAuthorizedById());
                }
                // We don't update created_by or created_date for existing records
                
                concessionRepo.save(concession);
            }
        }
        
     // --- 8. NEW: Save Payment Details (if provided) ---
        PaymentDetailsDTO paymentDTO =dto.getPaymentDetails();
 
		if (paymentDTO != null && paymentDTO.getAmount() != null) {
		    PaymentDetails paymentDetails = new PaymentDetails();
 
		    paymentDetails.setStudentAcademicDetails(student);
		    paymentDetails.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
		    paymentDetails.setPre_print_receipt_no(paymentDTO.getPrePrintedReceiptNo());
		    
		    // Use the Remarks from the DTO, which can come from either UI
		    paymentDetails.setRemarks(paymentDTO.getRemarks());
		    
		    paymentDetails.setCreated_by(paymentDTO.getCreatedBy());
		    paymentDetails.setApp_fee(paymentDTO.getAmount());
		    paymentDetails.setPaid_amount(paymentDTO.getAmount());
 
		    paymentDetails.setAcedemicYear(student.getAcademicYear());
		    paymentDetails.setStudentClass(student.getStudentClass());
 
		    // Set the PaymentMode (e.g., Cash, DD, Cheque)
		    if (paymentDTO.getPaymentModeId() != null) {
		        paymentModeRepo.findById(paymentDTO.getPaymentModeId())
		            .ifPresent(paymentDetails::setPaymenMode);
		    }
		        student.setStatus(activeStatus);
		
 
		    // Use the same default status you found earlier (ID: 2)
		    paymentDetails.setStatus(activeStatus);
 
		    // 1. SAVE THE MAIN PAYMENT RECORD
		    PaymentDetails savedPaymentDetails = paymentDetailsRepo.save(paymentDetails);
 
		    // 2. CHECK IF A TRANSACTION RECORD IS ALSO NEEDED (for DD or Cheque)
		    Integer paymentModeId = paymentDTO.getPaymentModeId();
 
		    // --- IMPORTANT: VERIFY THESE IDs FROM YOUR 'payment_mode' TABLE ---
		    final int DD_PAYMENT_ID = 2;     // Example ID for 'DD'
		    final int CHEQUE_PAYMENT_ID = 3; // Example ID for 'Cheque'
		    // ------------------------------------------------------------------
 
		    if (paymentModeId != null && (paymentModeId == DD_PAYMENT_ID || paymentModeId == CHEQUE_PAYMENT_ID)) {
		        
		        StudentApplicationTransaction transaction = new StudentApplicationTransaction();
		        
		        // Link to the main payment record
		        transaction.setPaymentDetails(savedPaymentDetails);
		        transaction.setPaymentMode(savedPaymentDetails.getPaymenMode());
		        
		        // Set common transaction fields
		        transaction.setNumber(paymentDTO.getTransactionNumber()); // DD No or Cheque No
		        transaction.setDate(paymentDTO.getTransactionDate());     // DD Date or Cheque Date
		        transaction.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
		        transaction.setCreated_by(paymentDTO.getCreatedBy());
		        transaction.setStatus("Pending"); // Set a default status, e.g., "Pending" or "Submitted"
 
		        if (paymentModeId == DD_PAYMENT_ID) {
		            // --- Set DD-Specific Fields ---
		            if (paymentDTO.getOrganisationId() != null) {
		                transaction.setOrg_id(paymentDTO.getOrganisationId());
		            }
		            if (paymentDTO.getBankId() != null) {
		                orgBankRepo.findById(paymentDTO.getBankId()).ifPresent(transaction::setOrgBank);
		            }
		            if (paymentDTO.getBranchId() != null) {
		                orgBankBranchRepo.findById(paymentDTO.getBranchId()).ifPresent(transaction::setOrgBankBranch);
		            }
		            transaction.setIfsc_code(paymentDTO.getIfscCode());
		            
		            if (paymentDTO.getCityId() != null) {
		                cityRepo.findById(paymentDTO.getCityId()).ifPresent(transaction::setCity);
		            }
		            
		        } else if (paymentModeId == CHEQUE_PAYMENT_ID) {
		            // --- Set Cheque-SpecFfic Fields ---
		            transaction.setIfsc_code(paymentDTO.getIfscCode());
		            if (paymentDTO.getOrganisationId() != null) {
		                transaction.setOrg_id(paymentDTO.getOrganisationId());
		            }
		            if (paymentDTO.getCityId() != null) {
		                cityRepo.findById(paymentDTO.getCityId()).ifPresent(transaction::setCity);
		            }
		            // Assuming Cheque also uses Bank and Branch, based on your UI and entity
		            if (paymentDTO.getBankId() != null) {
		                orgBankRepo.findById(paymentDTO.getBankId()).ifPresent(transaction::setOrgBank);
		            }
		            if (paymentDTO.getBranchId() != null) {
		                orgBankBranchRepo.findById(paymentDTO.getBranchId()).ifPresent(transaction::setOrgBankBranch);
		            }
		        }
 
		        // 3. SAVE THE TRANSACTION RECORD
		        studentApplicationTransactionRepo.save(transaction);
		    }
		}
        return student;
    }
}