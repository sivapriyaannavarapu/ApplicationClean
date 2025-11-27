package com.application.service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.application.dto.AppNumberRangeDTO;
import com.application.dto.DgmToCampusFormDTO;
import com.application.dto.GenericDropdownDTO;
import com.application.entity.AdminApp;
import com.application.entity.BalanceTrack;
import com.application.entity.CampusProView;
import com.application.entity.Distribution;
import com.application.entity.District;
import com.application.entity.UserAdminView;
import com.application.repository.AcademicYearRepository;
import com.application.repository.AdminAppRepository;
import com.application.repository.AppIssuedTypeRepository;
import com.application.repository.BalanceTrackRepository;
import com.application.repository.CampaignRepository;
import com.application.repository.CampusProViewRepository;
import com.application.repository.CampusRepository;
import com.application.repository.CityRepository;
import com.application.repository.DgmRepository;
import com.application.repository.DistributionRepository;
import com.application.repository.DistrictRepository;
import com.application.repository.EmployeeRepository;
import com.application.repository.StateRepository;
import com.application.repository.UserAdminViewRepository;
import com.application.repository.ZonalAccountantRepository;

import lombok.NonNull;

@Service
public class CampusService {

	private final AcademicYearRepository academicYearRepository;
	private final StateRepository stateRepository;
	private final DistrictRepository districtRepository;
	private final CityRepository cityRepository;
	private final CampusRepository campusRepository;
	private final CampaignRepository campaignRepository;
	private final AppIssuedTypeRepository appIssuedTypeRepository;
	private final DistributionRepository distributionRepository;
	private final EmployeeRepository employeeRepository;
	private final BalanceTrackRepository balanceTrackRepository;
	private final CampusProViewRepository campusProViewRepository;

	@Autowired
	UserAdminViewRepository userAdminViewRepository;

	@Autowired
	private AdminAppRepository adminAppRepository;

	@Autowired
	private ZonalAccountantRepository zonalAccountantRepository;

	@Autowired
	private DgmRepository dgmRepository;

	public CampusService(AcademicYearRepository academicYearRepository, StateRepository stateRepository,
			DistrictRepository districtRepository, CityRepository cityRepository, CampusRepository campusRepository,
			CampaignRepository campaignRepository, AppIssuedTypeRepository appIssuedTypeRepository,
			DistributionRepository distributionRepository, EmployeeRepository employeeRepository,
			BalanceTrackRepository balanceTrackRepository, CampusProViewRepository campusProViewRepository) {
		this.academicYearRepository = academicYearRepository;
		this.stateRepository = stateRepository;
		this.districtRepository = districtRepository;
		this.cityRepository = cityRepository;
		this.campusRepository = campusRepository;
		this.campaignRepository = campaignRepository;
		this.appIssuedTypeRepository = appIssuedTypeRepository;
		this.distributionRepository = distributionRepository;
		this.employeeRepository = employeeRepository;
		this.balanceTrackRepository = balanceTrackRepository;
		this.campusProViewRepository = campusProViewRepository;
	}

	// --- Dropdowns & Helpers with caching ---
//    @Cacheable("academicYears")
	public List<GenericDropdownDTO> getAllAcademicYears() {
		return academicYearRepository.findAll().stream()
				.map(y -> new GenericDropdownDTO(y.getAcdcYearId(), y.getAcademicYear())).collect(Collectors.toList());
	}

//    @Cacheable("states")
	public List<GenericDropdownDTO> getAllStates() {
		return stateRepository.findAll().stream().map(s -> new GenericDropdownDTO(s.getStateId(), s.getStateName()))
				.collect(Collectors.toList());
	}

	@Cacheable("districts")
	public List<District> getAllDistricts() {
		return districtRepository.findAll();
	}

//    @Cacheable(cacheNames = "districtsByState", key = "#stateId")
	public List<GenericDropdownDTO> getDistrictsByStateId(int stateId) {
		return districtRepository.findByStateStateId(stateId).stream()
				.map(d -> new GenericDropdownDTO(d.getDistrictId(), d.getDistrictName())).collect(Collectors.toList());
	}

	@Cacheable(cacheNames = "citiesByDistrict", key = "#districtId")
	public List<GenericDropdownDTO> getCitiesByDistrictId(int districtId) {

		final int ACTIVE_STATUS = 1;
		return cityRepository.findByDistrictDistrictIdAndStatus(districtId, ACTIVE_STATUS).stream()
				.map(c -> new GenericDropdownDTO(c.getCityId(), c.getCityName())).collect(Collectors.toList());
	}

	@Cacheable(cacheNames = "campusesByCity", key = "#cityId")
	public List<GenericDropdownDTO> getCampusesByCityId(int cityId) {
		return campusRepository.findByCityCityId(cityId).stream()
				.map(c -> new GenericDropdownDTO(c.getCampusId(), c.getCampusName())).collect(Collectors.toList());
	}

//    @Cacheable("campaignAreas")
	public List<GenericDropdownDTO> getAllCampaignAreas() {
		return campaignRepository.findAll().stream()
				.map(c -> new GenericDropdownDTO(c.getCampaignId(), c.getAreaName())).collect(Collectors.toList());
	}

//    @Cacheable(cacheNames = "prosByCampus", key = "#campusId")
	public List<GenericDropdownDTO> getProsByCampusId(int campusId) {
		List<Integer> employeeIds = campusProViewRepository.findByCampusId(campusId).stream()
				.map(CampusProView::getCmps_emp_id).toList();
		if (employeeIds.isEmpty())
			return List.of();
		return employeeRepository.findAllById(employeeIds).stream()
				.map(e -> new GenericDropdownDTO(e.getEmp_id(), e.getFirst_name() + " " + e.getLast_name()))
				.collect(Collectors.toList());
	}

	@Cacheable(cacheNames = "prosByCampus", key = "#campusId")
	public List<GenericDropdownDTO> getEmployeeDropdownByCampus(int campusId) {

		// 1. Get the list of emp_id's from CampusProView based on campusId
		List<Integer> employeeIds = campusProViewRepository.findEmployeeIdsByCampusId(campusId);

		// Check if any IDs were found to avoid running an empty IN clause
		if (employeeIds.isEmpty()) {
			return List.of(); // Return an empty list
		}

		// 2. Use the list of IDs to fetch the employee details and map to DTO
		return employeeRepository.findEmployeesByIdsAsDropdown(employeeIds);
	}

//    @Cacheable("issuedToTypes")
	public List<GenericDropdownDTO> getAllIssuedToTypes() {
		return appIssuedTypeRepository.findAll().stream()
				.map(t -> new GenericDropdownDTO(t.getAppIssuedId(), t.getTypeName())).collect(Collectors.toList());
	}

//    @Cacheable(cacheNames = "availableAppNumberRanges", key = "{#employeeId, #academicYearId}")
	public List<AppNumberRangeDTO> getAvailableAppNumberRanges(int employeeId, int academicYearId) {
		return balanceTrackRepository.findAppNumberRanges(academicYearId, employeeId).stream()
				.map(r -> new AppNumberRangeDTO(r.getAppBalanceTrkId(), r.getAppFrom(), r.getAppTo()))
				.collect(Collectors.toList());
	}

//    @Cacheable(cacheNames = "mobileNumberByEmpId", key = "#empId")
	public String getMobileNumberByEmpId(int empId) {
		return employeeRepository.findMobileNoByEmpId(empId);
	}

//    @Cacheable(cacheNames = "campusByCampaign", key = "#campaignId")
	public List<GenericDropdownDTO> getCampusByCampaignId(int campaignId) {
		return campusRepository.findCampusByCampaignId(campaignId).stream()
				.map(c -> new GenericDropdownDTO(c.getCampusId(), c.getCampusName())).toList();
	}

//    @Cacheable(cacheNames = "campaignsByCity", key = "#cityId")
	public List<GenericDropdownDTO> getCampaignsByCityId(int cityId) {
		return campaignRepository.findByCity_CityId(cityId).stream()
				.map(c -> new GenericDropdownDTO(c.getCampaignId(), c.getAreaName())).toList();
	}

	private int getDgmUserTypeId(int userId) {
		// 1. Fetch all roles for this user
		List<UserAdminView> userRoles = userAdminViewRepository.findRolesByEmpId(userId);

		if (userRoles.isEmpty()) {
			// Fallback: If no roles found (rare for internal staff), throw error or default
			throw new RuntimeException("No roles found for Employee ID: " + userId);
		}

		int highestPriorityTypeId = Integer.MAX_VALUE;

		// 2. Loop through roles to find the "Highest Rank" (Smallest ID number)
		for (UserAdminView userView : userRoles) {
			if (userView.getRole_name() == null)
				continue;

			String normalizedRoleName = userView.getRole_name().trim().toUpperCase();

			int currentTypeId = switch (normalizedRoleName) {
			case "ADMIN" -> 1;
			case "ZONAL ACCOUNTANT" -> 2;
			case "DGM" -> 3;
			// Add other roles here if needed
			default -> -1;
			};

			// Logic: If we found a valid role (not -1) AND it is higher rank (smaller
			// number)
			if (currentTypeId != -1 && currentTypeId < highestPriorityTypeId) {
				highestPriorityTypeId = currentTypeId;
			}
		}

		if (highestPriorityTypeId == Integer.MAX_VALUE) {
			throw new IllegalArgumentException(
					"User " + userId + " does not have a valid Issuer Role (Admin/Zone/DGM).");
		}

		return highestPriorityTypeId;
	}

	@Transactional
    public void submitDgmToCampusForm(@NonNull DgmToCampusFormDTO formDto) {
        int dgmUserId = formDto.getUserId();
        int dgmUserTypeId = getDgmUserTypeId(dgmUserId);

        // -------------------------------------------------------
        // STEP 1: CHECK FOR OVERLAPS (This was missing!)
        // -------------------------------------------------------
        int startNo = Integer.parseInt(formDto.getApplicationNoFrom());
        int endNo = Integer.parseInt(formDto.getApplicationNoTo());

        // 1. Find conflicts
        List<Distribution> overlappingDists = distributionRepository.findOverlappingDistributions(
                formDto.getAcademicYearId(), startNo, endNo);

        // 2. Execute the Split/Recall Logic
        if (!overlappingDists.isEmpty()) {
            // This call removes the warning and fixes the logic!
            handleOverlappingDistributions(overlappingDists, formDto);
        }
        // -------------------------------------------------------

        // 2. Create and Map Basic Fields
        Distribution distribution = new Distribution();
        mapDtoToDistribution(distribution, formDto, dgmUserTypeId); 
        
        // 3. Lookup Receiver
        CampusProView receiver = campusProViewRepository.findByEmp_id(formDto.getReceiverId())
                .orElseThrow(() -> new RuntimeException("Receiver not found"));

        // 4. Traffic Switch
        if (receiver.getIsOurEmp() == 1) {
            distribution.setIssued_to_emp_id(receiver.getCmps_emp_id());
            distribution.setIssued_to_pro_id(null); 
        } else {
            distribution.setIssued_to_pro_id(receiver.getCmps_emp_id()); 
            distribution.setIssued_to_emp_id(null);
        }

        // 5. Save and Flush
        Distribution savedDist = distributionRepository.saveAndFlush(distribution);

        // 6. Recalculate Balances
        int stateId = savedDist.getState().getStateId(); 
        Float amount = formDto.getApplication_Amount(); 

        // A. Update Issuer
        recalculateBalanceForEmployee(dgmUserId, formDto.getAcademicYearId(), stateId, dgmUserTypeId, dgmUserId, amount);
        
        // B. Update Receiver
        addStockToReceiver(savedDist, formDto.getAcademicYearId(), formDto.getIssuedToTypeId(), dgmUserId, amount);
    }

	private Integer getIssuerZoneId(int issuerId, int issuerTypeId) {
		// If Issuer is Zonal Accountant (Type 2)
		if (issuerTypeId == 2) {
			// Use ZonalAccountantRepository to find the Zone
			return zonalAccountantRepository.findZoneIdByEmployeeId(issuerId).stream().findFirst()
					.orElseThrow(() -> new RuntimeException("Issuer is a Zone Officer but not mapped to any Zone"));
		}

		// If Issuer is DGM (Type 3)
		if (issuerTypeId == 3) {
			// DGM -> Campus -> Zone
			// Assuming you have a method in DgmRepository or similar
			// OR we can check ZonalAccountant repo if DGMs are listed there too.
			// Let's assume DgmRepository has this:
			/*
			 * return dgmRepository.findZoneIdByDgmEmpId(issuerId);
			 */

			// WORKAROUND if you don't have that specific query yet:
			// We can use the 'branchId' from the form context IF the DGM is operating from
			// their own branch.
			// But safer to look up the DGM entity:
			// Dgm dgm = dgmRepository.findByEmpId(issuerId).get(0);
			// return dgm.getCampus().getZone().getZoneId();

			// Placeholder: You need to ensure you can get the Zone ID for the DGM
			return dgmRepository.findZoneIdByEmpId(issuerId)
					.orElseThrow(() -> new RuntimeException("Issuer is a DGM but not mapped to any Zone"));
		}

		return null;
	}

	private void recalculateBalanceForPro(int proId, int academicYearId, int stateId, int typeId, int createdBy, Float amount) {

	    // 1. Find existing PRO balance
	    BalanceTrack balance = balanceTrackRepository.findActiveBalanceByProAndAmount(academicYearId, proId, amount)
	            .orElseGet(() -> {
	                BalanceTrack nb = new BalanceTrack();
	                nb.setIssuedToProId(proId);
	                nb.setEmployee(null); 
	                nb.setAcademicYear(academicYearRepository.findById(academicYearId).orElseThrow());
	                nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());
	                nb.setIsActive(1);
	                nb.setCreatedBy(createdBy);
	                nb.setAmount(amount);
	                return nb;
	            });

	    // 2. Calculate Totals from Distribution Table
	    // This query returns '7' correctly
	    Integer totalReceived = distributionRepository
	            .sumTotalAppCountByIssuedToProIdAndAmount(proId, academicYearId, amount).orElse(0);

	    // 3. Set Ranges AND Count
	    if (totalReceived > 0) {
	        // These two lines worked (that's why you see 111-117)
	        balance.setAppFrom(
	                distributionRepository.findMinAppStartNoByIssuedToProId(proId, academicYearId).orElse(0));
	        balance.setAppTo(
	                distributionRepository.findMaxAppEndNoByIssuedToProId(proId, academicYearId).orElse(0));
	        
	        // --- THIS IS THE MISSING LINE ---
	        balance.setAppAvblCnt(totalReceived); 
	        // --------------------------------
	        
	    } else {
	        balance.setAppFrom(0);
	        balance.setAppTo(0);
	        balance.setAppAvblCnt(0);
	    }

	    balanceTrackRepository.save(balance);
	}

	@Transactional
	public void updateDgmToCampusForm(@NonNull Integer distributionId, @NonNull DgmToCampusFormDTO formDto) {
	    System.out.println("--- LOG: START updateDgmToCampusForm ---");
	    System.out.println("--- LOG: Dist ID: " + distributionId + ", User ID: " + formDto.getUserId() + 
	                       ", Target Range: " + formDto.getApplicationNoFrom() + "-" + formDto.getApplicationNoTo());

	    // 1. Fetch Existing Record
	    Distribution existingDistribution = distributionRepository.findById(distributionId)
	            .orElseThrow(() -> new RuntimeException("Distribution record not found for ID: " + distributionId));

	    // 2. Extract Critical Data
	    Float originalAmount = existingDistribution.getAmount();
	    int stateId = existingDistribution.getState().getStateId();
	    int dgmUserId = formDto.getUserId();
	    int academicYearId = formDto.getAcademicYearId();
	    int dgmUserTypeId = getDgmUserTypeId(dgmUserId);

	    // 3. Resolve New Receiver
	    CampusProView newReceiver = campusProViewRepository.findByEmp_id(formDto.getReceiverId())
	            .orElseThrow(() -> new RuntimeException("New Receiver not found"));

	    // School Validation
	    if (dgmUserTypeId != 1) { 
	        String category = formDto.getCategory();
	        if (category != null && category.trim().equalsIgnoreCase("SCHOOL")) {
	            Integer targetZoneId = newReceiver.getZoneId();
	            Integer myZoneId = getIssuerZoneId(dgmUserId, dgmUserTypeId);
	            if (targetZoneId != null && myZoneId != null && !targetZoneId.equals(myZoneId)) {
	                throw new RuntimeException("Update Denied: For SCHOOL category, cannot transfer to a different Zone.");
	            }
	        }
	    }

	    // Determine New IDs
	    Integer newEmpId = (newReceiver.getIsOurEmp() == 1) ? newReceiver.getCmps_emp_id() : null;
	    Integer newProId = (newReceiver.getIsOurEmp() == 0) ? newReceiver.getCmps_emp_id() : null;

	    // Identify Changes
	    Integer oldEmpId = existingDistribution.getIssued_to_emp_id();
	    Integer oldProId = existingDistribution.getIssued_to_pro_id();
	    boolean isRecipientChanging = !java.util.Objects.equals(oldEmpId, newEmpId) || !java.util.Objects.equals(oldProId, newProId);

	    int oldStart = (int) existingDistribution.getAppStartNo();
	    int oldEnd = (int) existingDistribution.getAppEndNo();
	    int newStart = Integer.parseInt(formDto.getApplicationNoFrom());
	    int newEnd = Integer.parseInt(formDto.getApplicationNoTo());
	    boolean isRangeChanging = oldStart != newStart || oldEnd != newEnd;

	    System.out.println("--- LOG: Range Changing? " + isRangeChanging + " (" + oldStart + "->" + newStart + ")");
	    System.out.println("--- LOG: Recipient Changing? " + isRecipientChanging);

	    // -------------------------------------------------------
	    // STEP 6: CHECK OVERLAPS (THE FIX LOGIC)
	    // -------------------------------------------------------
	    boolean isIssuerRebuilt = false; // Flag to track if we already fixed 4818

	    if (isRangeChanging || isRecipientChanging) {
	        System.out.println("--- LOG: Checking for overlaps...");
	        List<Distribution> overlappingDists = distributionRepository.findOverlappingDistributions(
	                academicYearId, newStart, newEnd);
	        
	        // Filter out self
	        List<Distribution> others = overlappingDists.stream()
	            .filter(d -> !d.getAppDistributionId().equals(distributionId))
	            .toList();

	        System.out.println("--- LOG: Found " + others.size() + " overlapping distributions (excluding self).");

	        if (!others.isEmpty()) {
	            // FIX: Capture the boolean return value!
	            isIssuerRebuilt = handleOverlappingDistributions(others, formDto);
	        }
	    }
	    
	    System.out.println("--- LOG: isIssuerRebuilt flag is: " + isIssuerRebuilt);

	    // -------------------------------------------------------
	    // STEP 7: EXECUTE UPDATE
	    // -------------------------------------------------------
	    
	    // A. Inactivate Old
	    existingDistribution.setIsActive(0);
	    distributionRepository.saveAndFlush(existingDistribution);

	    // B. Create New
	    Distribution newDist = new Distribution();
	    mapDtoToDistribution(newDist, formDto, dgmUserTypeId);
	    newDist.setIssued_to_emp_id(newEmpId);
	    newDist.setIssued_to_pro_id(newProId);
	    newDist.setAmount(originalAmount); 
	    distributionRepository.saveAndFlush(newDist);

	    // 8. Handle Remainders (For Shrinking/Shifting)
	    if (isRangeChanging) {
	        if (oldStart < newStart) {
	            System.out.println("--- LOG: Creating standard start remainder.");
	            createAndSaveRemainder(existingDistribution, oldStart, newStart - 1);
	        }
	        if (oldEnd > newEnd) {
	            System.out.println("--- LOG: Creating standard end remainder.");
	            createAndSaveRemainder(existingDistribution, newEnd + 1, oldEnd);
	        }
	    }

	    // -------------------------------------------------------
	    // STEP 9: RECALCULATE BALANCES
	    // -------------------------------------------------------

	    // A. Issuer (4818) - THE CRITICAL FIX
	    if (!isIssuerRebuilt) {
	        System.out.println("--- LOG: Performing Standard Issuer Recalculation (Flag was FALSE)");
	        recalculateBalanceForEmployee(dgmUserId, academicYearId, stateId, dgmUserTypeId, dgmUserId, originalAmount);
	    } else {
	        System.out.println("--- LOG: SKIPPING Issuer Recalculation (Flag was TRUE) - Balance 4818 is safe.");
	    }

	    // B. New Receiver
	    if (newProId != null) {
	        recalculateBalanceForPro(newProId, academicYearId, stateId, formDto.getIssuedToTypeId(), dgmUserId, originalAmount);
	    } else if (newEmpId != null) {
	        // Use smart addStockToReceiver here
	        addStockToReceiver(newDist, academicYearId, formDto.getIssuedToTypeId(), dgmUserId, originalAmount);
	    }

	    // C. Old Receiver (If changed)
	    if (isRecipientChanging || isRangeChanging) {
	        if (oldEmpId != null) {
	            recalculateBalanceForEmployee(oldEmpId, academicYearId, stateId, existingDistribution.getIssuedToType().getAppIssuedId(), dgmUserId, originalAmount);
	        }
	        if (oldProId != null) {
	            recalculateBalanceForPro(oldProId, academicYearId, stateId, existingDistribution.getIssuedToType().getAppIssuedId(), dgmUserId, originalAmount);
	        }
	    }
	    
	    System.out.println("--- LOG: END updateDgmToCampusForm ---");
	}

	// --- PRIVATE HELPER METHODS ---

	// CHANGE: Return type must be boolean
	private boolean handleOverlappingDistributions(List<Distribution> overlappingDists, DgmToCampusFormDTO request) {
	    System.out.println("--- LOG: Inside handleOverlappingDistributions ---");
	    System.out.println("--- LOG: Request User ID: " + request.getUserId());
	    
	    boolean issuerRebuilt = false; 
	    int reqStart = Integer.parseInt(request.getApplicationNoFrom());
	    int reqEnd = Integer.parseInt(request.getApplicationNoTo());

	    for (Distribution oldDist : overlappingDists) {
	        Integer oldHolderId = null;
	        boolean isPro = false;
	        
	        if (oldDist.getIssued_to_emp_id() != null) {
	            oldHolderId = oldDist.getIssued_to_emp_id();
	        } else if (oldDist.getIssued_to_pro_id() != null) {
	            oldHolderId = oldDist.getIssued_to_pro_id();
	            isPro = true;
	        }

	        System.out.println("--- LOG: Processing Overlap. Old Holder ID: " + oldHolderId + ", IsPro: " + isPro);

	        // 2. Inactivate OLD
	        oldDist.setIsActive(0);
	        distributionRepository.saveAndFlush(oldDist); 

	        // 3. Split Logic
	        int oldStart = oldDist.getAppStartNo();
	        int oldEnd = oldDist.getAppEndNo();

	        if (oldStart < reqStart) {
	            System.out.println("--- LOG: Creating Start Remainder: " + oldStart + " to " + (reqStart - 1));
	            createAndSaveRemainder(oldDist, oldStart, reqStart - 1);
	        }
	        if (oldEnd > reqEnd) {
	            System.out.println("--- LOG: Creating End Remainder: " + (reqEnd + 1) + " to " + oldEnd);
	            createAndSaveRemainder(oldDist, reqEnd + 1, oldEnd);
	        }

	        // 4. Recalculate Victim's Balance
	        if (isPro) {
	            recalculateBalanceForPro(oldHolderId, request.getAcademicYearId(), 
	                    oldDist.getState().getStateId(), oldDist.getIssuedToType().getAppIssuedId(), 
	                    request.getUserId(), oldDist.getAmount());
	        } else {
	            System.out.println("--- LOG: Rebuilding Balance for Emp ID: " + oldHolderId);
	            
	            rebuildBalancesFromDistributions(oldHolderId, request.getAcademicYearId(), 
	                    oldDist.getIssuedToType().getAppIssuedId(), request.getUserId(), oldDist.getAmount());

	            // --- CRITICAL FIX HERE: Use .equals() instead of == ---
	            if (oldHolderId != null && oldHolderId.equals(request.getUserId())) {
	                System.out.println("--- LOG: MATCH FOUND! Issuer " + oldHolderId + " was rebuilt.");
	                issuerRebuilt = true; 
	            } else {
	                System.out.println("--- LOG: No Match. OldHolder: " + oldHolderId + " vs RequestUser: " + request.getUserId());
	            }
	        }
	    }
	    
	    System.out.println("--- LOG: Exiting handleOverlappingDistributions. Returns: " + issuerRebuilt);
	    return issuerRebuilt;
	}
	// REVISED recalculateBalanceForEmployee for CampusService
	// REVISED recalculateBalanceForEmployee in DgmService
private void recalculateBalanceForEmployee(int employeeId, int academicYearId, int stateId, int typeId, int createdBy, Float amount) {
        
        // 1. CHECK: Is this a CO/Admin? (Check Master Table)
        Optional<AdminApp> adminApp = adminAppRepository.findByEmpAndYearAndAmount(
                employeeId, academicYearId, amount);

        if (adminApp.isPresent()) {
            // CASE A: CO / ADMIN (Source Logic)
            AdminApp master = adminApp.get();
            
            List<BalanceTrack> balances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(
                    academicYearId, employeeId, amount);
            
            BalanceTrack balance;
            if (balances.isEmpty()) {
                balance = createNewBalanceTrack(employeeId, academicYearId, typeId, createdBy);
                balance.setAmount(amount);
                balance.setEmployee(employeeRepository.findById(employeeId).orElseThrow());
                balance.setIssuedToProId(null); 
            } else {
                balance = balances.get(0); 
            }

            int totalDistributed = distributionRepository.sumTotalAppCountByCreatedByAndAmount(
                    employeeId, academicYearId, amount).orElse(0);
            
            balance.setAppFrom(master.getAppFromNo());
            balance.setAppTo(master.getAppToNo());
            balance.setAppAvblCnt(master.getTotalApp() - totalDistributed);
            
            balanceTrackRepository.save(balance);
        
        } else {
            // CASE B: INTERMEDIARY (DGM/Zone) -> REBUILD LOGIC
            // This fixes the issue where sender balance wasn't updating correctly for gaps
            rebuildBalancesFromDistributions(employeeId, academicYearId, typeId, createdBy, amount);
        }
    }
	
private void rebuildBalancesFromDistributions(int empId, int acYearId, int typeId, int createdBy, Float amount) {
    System.out.println("--- LOG: Inside rebuildBalancesFromDistributions for Emp: " + empId);

    // 1. Fetch Active Distributions (IGNORING AMOUNT IN SQL)
    // This fixes the bug where "2000.0" != "2000" in the database
    List<Distribution> allHoldings = distributionRepository.findActiveHoldingsForEmp(empId, acYearId);
    
    System.out.println("--- LOG: Raw active holdings found: " + allHoldings.size());

    // 2. Filter by Amount in Java (Safe Float Comparison)
    List<Distribution> holdings = allHoldings.stream()
            .filter(d -> Math.abs(d.getAmount() - amount) < 0.01) // Tolerates tiny differences
            .toList();

    System.out.println("--- LOG: Holdings after Amount filter: " + holdings.size());
    
    // 3. Fetch Current Balance Tracks to wipe them clean
    // (We keep the DB query here as is, or you can relax it too if needed, but usually the issue is finding the source distribution)
    List<BalanceTrack> currentBalances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(
            acYearId, empId, amount);

    System.out.println("--- LOG: Found " + currentBalances.size() + " existing balance tracks to deactivate.");

    // 4. Deactivate old balances
    for (BalanceTrack b : currentBalances) {
        b.setIsActive(0);
        balanceTrackRepository.save(b);
    }
    
    // 5. Create NEW Balance Tracks
    if (holdings.isEmpty()) {
        System.out.println("--- LOG: WARNING! No holdings found. User balance will be 0.");
    }

    for (Distribution dist : holdings) {
        System.out.println("--- LOG: Creating NEW Balance Track for range: " + dist.getAppStartNo() + "-" + dist.getAppEndNo());
        
        // Pass 'false' because this method is specifically for Employees
        BalanceTrack nb = createNewBalanceTrack(empId, acYearId, dist.getIssuedByType().getAppIssuedId(), createdBy, false);
        
        nb.setAmount(amount);
        nb.setAppFrom((int) dist.getAppStartNo());
        nb.setAppTo((int) dist.getAppEndNo());
        nb.setAppAvblCnt(dist.getTotalAppCount());
        
        balanceTrackRepository.save(nb);
    }
}

	private void mapDtoToDistribution(Distribution distribution, DgmToCampusFormDTO formDto, int issuedById) {
		int appNoFrom = Integer.parseInt(formDto.getApplicationNoFrom());
		int appNoTo = Integer.parseInt(formDto.getApplicationNoTo());

		// Basic Mappings
		academicYearRepository.findById(formDto.getAcademicYearId()).ifPresent(distribution::setAcademicYear);

		cityRepository.findById(formDto.getCityId()).ifPresent(city -> {
			distribution.setCity(city);
			if (city.getDistrict() != null) {
				distribution.setDistrict(city.getDistrict());
				if (city.getDistrict().getState() != null) {
					distribution.setState(city.getDistrict().getState());
				}
			}
		});

		// Set Branch Context (Location)
		campusRepository.findById(formDto.getBranchId()).ifPresent(c -> {
			distribution.setCampus(c);
			distribution.setZone(c.getZone());
		});

		// Types
		appIssuedTypeRepository.findById(issuedById).ifPresent(distribution::setIssuedByType);

		// USE FRONTEND VALUE
		appIssuedTypeRepository.findById(formDto.getIssuedToTypeId()).ifPresent(distribution::setIssuedToType);

		distribution.setAppStartNo(appNoFrom);
		distribution.setAppEndNo(appNoTo);
		distribution.setTotalAppCount(formDto.getRange());
		distribution.setAmount(formDto.getApplication_Amount());

		// Date Logic: Use Frontend Date, fallback to Now
		if (formDto.getIssueDate() != null) {
			distribution.setIssueDate(formDto.getIssueDate().atStartOfDay()); // Convert to LocalDateTime
		} else {
			distribution.setIssueDate(java.time.LocalDateTime.now());
		}

		distribution.setIsActive(1);
		distribution.setCreated_by(formDto.getUserId());
	}
	
	

	private void mapExistingToNewDistribution(Distribution newDist, Distribution oldDist) {
		newDist.setAcademicYear(oldDist.getAcademicYear());
		newDist.setState(oldDist.getState());
		newDist.setDistrict(oldDist.getDistrict());
		newDist.setCity(oldDist.getCity());
		newDist.setZone(oldDist.getZone());
		newDist.setCampus(oldDist.getCampus());
		newDist.setIssuedByType(oldDist.getIssuedByType());
		newDist.setIssuedToType(oldDist.getIssuedToType());
		newDist.setIssued_to_emp_id(oldDist.getIssued_to_emp_id());
		newDist.setAppStartNo(oldDist.getAppStartNo());
		newDist.setAppEndNo(oldDist.getAppEndNo());
		newDist.setTotalAppCount(oldDist.getTotalAppCount());
		newDist.setIssueDate(oldDist.getIssueDate());
		newDist.setIsActive(1);
		newDist.setCreated_by(oldDist.getCreated_by());
	}

	private BalanceTrack createNewBalanceTrack(int employeeId, int academicYearId, int typeId, int createdBy) {
		BalanceTrack nb = new BalanceTrack();
		// Basic setup only, specific IDs set by caller
		nb.setAcademicYear(academicYearRepository.findById(academicYearId).orElseThrow());
		nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());
		nb.setAppAvblCnt(0);
		nb.setIsActive(1);
		nb.setCreatedBy(createdBy);
		return nb;
	}

	// FIX: Renamed to match the loop call, added start/end args, added saveAndFlush
    private void createAndSaveRemainder(Distribution originalDist, int start, int end) {
        Distribution remainder = new Distribution();
        mapExistingToNewDistribution(remainder, originalDist);

        // CRITICAL: Copy EXACTLY who had it before (whether Employee OR Pro)
        remainder.setIssued_to_emp_id(originalDist.getIssued_to_emp_id());
        remainder.setIssued_to_pro_id(originalDist.getIssued_to_pro_id());

        // Set the specific split range
        remainder.setAppStartNo(start);
        remainder.setAppEndNo(end);
        remainder.setTotalAppCount((end - start) + 1);
        
        remainder.setIsActive(1);
        remainder.setAmount(originalDist.getAmount()); // Keep Amount

        // CRITICAL: Force DB to write NOW so recalculateBalance sees it
        distributionRepository.saveAndFlush(remainder);
    }

	// Smart Method to Add Stock (Handles Gaps & Receiver Type)
    private void addStockToReceiver(Distribution savedDist, int academicYearId, int typeId, int createdBy, Float amount) {
        int newStart = savedDist.getAppStartNo();
        int newEnd = savedDist.getAppEndNo();
        int newCount = savedDist.getTotalAppCount();
        int targetEnd = newStart - 1;
        
        boolean isPro = (savedDist.getIssued_to_pro_id() != null);
        Integer receiverId = isPro ? savedDist.getIssued_to_pro_id() : savedDist.getIssued_to_emp_id();

        if (receiverId == null) return;

        List<BalanceTrack> existingDuplicates;
        
        // FIX: Distinguish between Pro and Emp repo calls to prevent ID collisions
        if (isPro) {
            // Assuming you have this method or a generic find that works for both by column
            existingDuplicates = balanceTrackRepository.findActiveBalancesByProAndAmount(academicYearId, receiverId, amount);
        } else {
            existingDuplicates = balanceTrackRepository.findActiveBalancesByEmpAndAmount(academicYearId, receiverId, amount);
        }
        
        for (BalanceTrack b : existingDuplicates) {
            if (b.getAppFrom() == newStart && b.getAppTo() == newEnd) {
                System.out.println("--- LOG: Duplicate balance detected. Skipping creation.");
                return; 
            }
        }

        Optional<BalanceTrack> mergeableRow;
        if (isPro) {
            mergeableRow = balanceTrackRepository.findMergeableRowForPro(academicYearId, receiverId, amount, targetEnd);
        } else {
            mergeableRow = balanceTrackRepository.findMergeableRowForEmployee(academicYearId, receiverId, amount, targetEnd);
        }

        if (mergeableRow.isPresent()) {
            BalanceTrack existing = mergeableRow.get();
            existing.setAppTo(newEnd);
            existing.setAppAvblCnt(existing.getAppAvblCnt() + newCount);
            balanceTrackRepository.save(existing);
        } else {
            BalanceTrack newRow = createNewBalanceTrack(receiverId, academicYearId, typeId, createdBy, isPro);
            newRow.setAmount(amount);
            newRow.setAppFrom(newStart);
            newRow.setAppTo(newEnd);
            newRow.setAppAvblCnt(newCount);
            balanceTrackRepository.save(newRow);
        }
    }

	// Helper to create the empty object correctly based on type
	private BalanceTrack createNewBalanceTrack(int id, int acYear, int typeId, int createdBy, boolean isPro) {
		BalanceTrack nb = new BalanceTrack();
		nb.setAcademicYear(academicYearRepository.findById(acYear).orElseThrow());
		nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());
		nb.setIsActive(1);
		nb.setCreatedBy(createdBy);

		if (isPro) {
			nb.setIssuedToProId(id);
			nb.setEmployee(null); // DB Constraint satisfied (nullable)
		} else {
			nb.setEmployee(employeeRepository.findById(id).orElseThrow());
			nb.setIssuedToProId(null);
		}
		return nb;
	}
	
	// Overload for Employees (Defaults isPro = false)
    private BalanceTrack createNewBalanceTrack_(int employeeId, int academicYearId, int typeId, int createdBy) {
        return createNewBalanceTrack(employeeId, academicYearId, typeId, createdBy, false);
    }
}