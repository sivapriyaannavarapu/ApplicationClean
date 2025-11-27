package com.application.service;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.application.dto.AppSeriesDTO;
import com.application.dto.FormSubmissionDTO;
import com.application.dto.GenericDropdownDTO;
import com.application.dto.LocationAutoFillDTO;
import com.application.entity.AdminApp;
import com.application.entity.BalanceTrack;
import com.application.entity.Campus;
import com.application.entity.City;
import com.application.entity.Dgm;
import com.application.entity.Distribution;
import com.application.entity.District;
import com.application.entity.UserAdminView;
import com.application.repository.AcademicYearRepository;
import com.application.repository.AdminAppRepository;
import com.application.repository.AppIssuedTypeRepository;
import com.application.repository.BalanceTrackRepository;
import com.application.repository.CampusRepository;
import com.application.repository.CityRepository;
import com.application.repository.DgmRepository;
import com.application.repository.DistributionRepository;
import com.application.repository.EmployeeRepository;
import com.application.repository.UserAdminViewRepository;
import com.application.repository.ZonalAccountantRepository;
import com.application.repository.ZoneRepository;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class DgmService {

	private final AcademicYearRepository academicYearRepository;
	private final CityRepository cityRepository;
	private final ZoneRepository zoneRepository;
	private final CampusRepository campusRepository;
	private final AppIssuedTypeRepository appIssuedTypeRepository;
	private final DistributionRepository distributionRepository;
	private final EmployeeRepository employeeRepository;
	private final BalanceTrackRepository balanceTrackRepository;
	private final DgmRepository dgmRepository;
	private final UserAdminViewRepository userAdminViewRepository;
	private final ZonalAccountantRepository zonalAccountantRepository;
	private final AdminAppRepository adminAppRepository;

	// --- Dropdown and Helper Methods with Caching ---
//    @Cacheable("academicYears")
	public List<GenericDropdownDTO> getAllAcademicYears() {
		return academicYearRepository.findAll().stream()
				.map(year -> new GenericDropdownDTO(year.getAcdcYearId(), year.getAcademicYear()))
				.collect(Collectors.toList());
	}

	@Cacheable("cities")
	public List<GenericDropdownDTO> getAllCities() {
		final int ACTIVE_STATUS = 1;

		return cityRepository.findByStatus(ACTIVE_STATUS).stream()
				.map(city -> new GenericDropdownDTO(city.getCityId(), city.getCityName())).collect(Collectors.toList());
	}

//    @Cacheable(cacheNames = "zonesByCity", key = "#cityId")
	public List<GenericDropdownDTO> getZonesByCityId(int cityId) {
		return zoneRepository.findByCityCityId(cityId).stream()
				.map(zone -> new GenericDropdownDTO(zone.getZoneId(), zone.getZoneName())).collect(Collectors.toList());
	}

	@Cacheable(cacheNames = "campusesByZone", key = "#zoneId")
	public List<GenericDropdownDTO> getCampusesByZoneId(int zoneId) {
		// Call the new repository method
		return campusRepository.findActiveCampusesByZoneId(zoneId).stream()
				// .distinct() might be useful here to ensure unique campuses if a campus is
				// linked to multiple active zonal accountants in the same zone
				.distinct().map(campus -> new GenericDropdownDTO(campus.getCampusId(), campus.getCampusName()))
				.collect(Collectors.toList());
	}

	@Cacheable(cacheNames = "campusforzonalaccountant", key = "#empId")
	public List<GenericDropdownDTO> getCampusesByEmployeeId(int empId) {
		List<Integer> zoneIds = zonalAccountantRepository.findZoneIdByEmployeeId(empId);
		if (zoneIds == null || zoneIds.isEmpty()) {
			return Collections.emptyList();
		}

		return zoneIds.stream().flatMap(zoneId -> getCampusesByZoneId(zoneId).stream()).distinct()
				.collect(Collectors.toList());
	}

	public List<GenericDropdownDTO> getActiveCampusesByEmpId(Integer empId) {
		List<Dgm> dgms = dgmRepository.findByEmpId(empId);

		// Map to GenericDropdownDTO — filtering only active campuses
		return dgms.stream().filter(
				d -> d.getCampus() != null && d.getCampus().getIsActive() != null && d.getCampus().getIsActive() == 1)
				.map(d -> new GenericDropdownDTO(d.getCampus().getCampusId(), d.getCampus().getCampusName()))
				.collect(Collectors.toList());
	}

	public List<GenericDropdownDTO> getActiveCampusesByEmployeeId(int empId) {
		List<Integer> zoneIds = zonalAccountantRepository.findZoneIdByEmployeeId(empId);
		if (zoneIds == null || zoneIds.isEmpty()) {
			return Collections.emptyList();
		}

		// Step 2: Get campus IDs for all zones
		List<Integer> campusIds = dgmRepository.findCampusIdsByZoneIds(zoneIds);
		if (campusIds == null || campusIds.isEmpty()) {
			return Collections.emptyList();
		}

		// Step 3: Get only active campuses
		return campusRepository.findActiveCampusesByIds(campusIds);
	}

//    @Cacheable("issuedToTypes")
	public List<GenericDropdownDTO> getAllIssuedToTypes() {
		return appIssuedTypeRepository.findAll().stream()
				.map(type -> new GenericDropdownDTO(type.getAppIssuedId(), type.getTypeName()))
				.collect(Collectors.toList());
	}

	@Cacheable(value = "mobileNumberByEmpId", key = "#empId")
	public String getMobileNumberByEmpId(int empId) {
		return employeeRepository.findMobileNoByEmpId(empId);
	}

	@Cacheable(cacheNames = "getDgmforCampus", key = "#campusId")
	public List<GenericDropdownDTO> getDgmEmployeesForCampus(int campusId) {
		// 1. Find the Campus by ID
		Optional<Campus> campusOptional = campusRepository.findById(campusId);

		if (campusOptional.isEmpty()) {
			return Collections.emptyList();
		}

		// 2. Get the Zone ID from the Campus
		Campus campus = campusOptional.get();
		int zoneId = campus.getZone().getZoneId();

		// 3. Find distinct DGM employees for that Zone, checking isActive = 1
		// Calling the updated repository method:
		return dgmRepository.findDistinctActiveEmployeesByZoneId(zoneId);
	}
	
public List<Double> getApplicationFees(int empId, int academicYearId) { // UPDATED SIGNATURE
	    
	    // 1. Check AdminApp table first (UPDATED CALL)
	    List<Integer> adminFees = adminAppRepository.findAmountsByEmpIdAndAcademicYear(empId, academicYearId);

	    // 2. If AdminApp has data, convert to Double and return
	    if (adminFees != null && !adminFees.isEmpty()) {
	        return adminFees.stream()
	                .map(Double::valueOf)
	                .collect(Collectors.toList());
	    }

	    // 3. If AdminApp is empty, check BalanceTrack table (UPDATED CALL)
	    List<Float> balanceFees = balanceTrackRepository.findAmountsByEmpIdAndAcademicYear(empId, academicYearId);

	    if (balanceFees != null && !balanceFees.isEmpty()) {
	        return balanceFees.stream()
	                .map(Double::valueOf)
	                .collect(Collectors.toList());
	    }

	    // 4. If both are empty, return an empty list
	    return Collections.emptyList();
	}


public LocationAutoFillDTO getAutoPopulateData(int empId, String category) {

    // 1️⃣ Only apply logic when category = "school"
    if (!"school".equalsIgnoreCase(category)) {
        return null;  
    }

    // 2️⃣ Get active DGM record for employee
    Dgm dgm = dgmRepository
            .findActiveDgm(empId, 1)
            .orElse(null);

    if (dgm == null) {
        return null;
    }

    // 3️⃣ DISTRICT (direct from Dgm table)
    District district = dgm.getDistrict();

    Integer districtId   = district != null ? district.getDistrictId() : null;
    String districtName  = district != null ? district.getDistrictName() : null;

    // 4️⃣ CITY (via Campus)
    Campus campus = dgm.getCampus();
    City city = (campus != null) ? campus.getCity() : null;

    Integer cityId   = city != null ? city.getCityId() : null;
    String cityName  = city != null ? city.getCityName() : null;

    // 5️⃣ Return final DTO
    return new LocationAutoFillDTO(cityId, cityName, districtId, districtName);
}
	


//	public Optional<AppFromDTO> getAppFromByEmployeeAndYear(int employeeId, int academicYearId) {
//		return balanceTrackRepository.getAppFromByEmployeeAndAcademicYear(employeeId, academicYearId);
//
//		// OR if using the @Query method:
//		// return balanceTrackRepository.getAppFromByEmployeeAndAcademicYear(employeeId,
//		// academicYearId);
//	}
//
////  @Cacheable(cacheNames = "getAppRange", key = "{#academicYearId, #employeeId}")
//	public AppRangeDTO getAppRange(int empId, int academicYearId) {
//		// Fetch distribution data
//		AppDistributionDTO distDTO = distributionRepository
//				.findActiveAppRangeByEmployeeAndAcademicYear(empId, academicYearId).orElse(null);
//
//		// Fetch balance track data (now returns AppFromDTO with the ID)
//		AppFromDTO fromDTO = balanceTrackRepository.getAppFromByEmployeeAndAcademicYear(empId, academicYearId)
//				.orElse(null);
//
//		if (distDTO == null && fromDTO == null) {
//			return null;
//		}
//
//		// Merge results into a single DTO
//		Integer appStartNo = distDTO != null ? distDTO.getAppStartNo() : null;
//		Integer appEndNo = distDTO != null ? distDTO.getAppEndNo() : null;
//
//		// Extract fields from the updated AppFromDTO
//		Integer appFrom = fromDTO != null ? fromDTO.getAppFrom() : null;
//		Integer appBalanceTrkId = fromDTO != null ? fromDTO.getAppBalanceTrkId() : null; // Extracted new ID
//
//		// Use the updated AppRangeDTO constructor
//		return new AppRangeDTO(appStartNo, appEndNo, appFrom, appBalanceTrkId);
//	}
//
//	public AppRangeDTO getAppRange(int empId, int academicYearId, Integer cityId) { // Updated signature
//		// Fetch distribution data
//		AppDistributionDTO distDTO = distributionRepository
//				.findActiveAppRangeByEmployeeAndAcademicYear(empId, academicYearId, cityId) // Pass cityId
//				.orElse(null);
//
//		// Fetch balance track data (now returns AppFromDTO with the ID)
//		AppFromDTO fromDTO = balanceTrackRepository.getAppFromByEmployeeAndAcademicYear(empId, academicYearId)
//				.orElse(null);
//
//		if (distDTO == null && fromDTO == null) {
//			return null;
//		}
//
//		// Merge results into a single DTO
//		Integer appStartNo = distDTO != null ? distDTO.getAppStartNo() : null;
//		Integer appEndNo = distDTO != null ? distDTO.getAppEndNo() : null;
//
//		// Extract fields from the updated AppFromDTO
//		Integer appFrom = fromDTO != null ? fromDTO.getAppFrom() : null;
//		Integer appBalanceTrkId = fromDTO != null ? fromDTO.getAppBalanceTrkId() : null; // Extracted new ID
//
//		// Use the updated AppRangeDTO constructor
//		return new AppRangeDTO(appStartNo, appEndNo, appFrom, appBalanceTrkId);
//	}

	public List<AppSeriesDTO> getActiveSeriesForReceiver(int receiverId, Double amount, boolean isPro) {
        if (isPro) {
            return balanceTrackRepository.findSeriesByProIdAndAmount(receiverId, amount);
        } else {
            return balanceTrackRepository.findSeriesByEmpIdAndAmount(receiverId, amount);
        }
    }
	
	 public Integer getDistributionIdBySeries(int receiverId, int start, int end, Double amount, boolean isPro) {
	        if (isPro) {
	            return distributionRepository.findIdByProAndRange(receiverId, start, end, amount)
	                    .orElseThrow(() -> new RuntimeException("No Active Distribution found for this PRO range."));
	        } else {
	            return distributionRepository.findIdByEmpAndRange(receiverId, start, end, amount)
	                    .orElseThrow(() -> new RuntimeException("No Active Distribution found for this Employee range."));
	        }
	    }
	 
	// Generic helper to find the highest priority Role ID for ANY employee (Issuer or Receiver)
    private int getRoleTypeIdByEmpId(int empId) {
        List<UserAdminView> userRoles = userAdminViewRepository.findRolesByEmpId(empId);
        
        if (userRoles.isEmpty()) {
            // If the receiver has no role, they might be a basic employee. 
            // You might want a default ID (e.g., 4) or throw an error.
            // For DGM Service, we expect them to be DGM (3).
            throw new RuntimeException("No valid roles found for Employee ID: " + empId);
        }

        int highestPriorityTypeId = Integer.MAX_VALUE;
        for (UserAdminView userView : userRoles) {
            String roleName = userView.getRole_name().trim().toUpperCase();
            int currentTypeId = switch (roleName) {
                case "ADMIN" -> 1;
                case "ZONAL ACCOUNTANT" -> 2;
                case "DGM" -> 3;
                // Add "PRO" or "AGENT" -> 4 if needed
                default -> -1;
            };
            if (currentTypeId != -1 && currentTypeId < highestPriorityTypeId) {
                highestPriorityTypeId = currentTypeId;
            }
        }
        
        if (highestPriorityTypeId == Integer.MAX_VALUE) {
             // Fallback or Error. If DGM service, maybe default to 3?
             return 3; // Defaulting to DGM if role logic is strict
        }
        return highestPriorityTypeId;
    }
    
	private int getIssuedTypeByUserId(int userId) {
        List<UserAdminView> userRoles = userAdminViewRepository.findRolesByEmpId(userId);
        if (userRoles.isEmpty()) throw new RuntimeException("No roles found for ID: " + userId);

        int highestPriorityTypeId = Integer.MAX_VALUE;
        for (UserAdminView userView : userRoles) {
            String roleName = userView.getRole_name().trim().toUpperCase();
            int currentTypeId = switch (roleName) {
                case "ADMIN" -> 1;
                case "ZONAL ACCOUNTANT" -> 2;
                case "DGM" -> 3;
                default -> -1;
            };
            if (currentTypeId != -1 && currentTypeId < highestPriorityTypeId) {
                highestPriorityTypeId = currentTypeId;
            }
        }
        return highestPriorityTypeId;
    }

	@Transactional
    public void submitForm(@NonNull FormSubmissionDTO formDto) {
        int issuerUserId = formDto.getUserId(); 
        int receiverEmpId = formDto.getDgmEmployeeId(); 

        // 1. AUTO-DETECT TYPES (Backend Logic)
        int issuedById = getRoleTypeIdByEmpId(issuerUserId);   // Who is sending?
        int issuedToId = getRoleTypeIdByEmpId(receiverEmpId);  // Who is receiving?

        // 2. Check Overlaps
        int startNo = Integer.parseInt(formDto.getApplicationNoFrom());
        int endNo = Integer.parseInt(formDto.getApplicationNoTo());
        List<Distribution> overlappingDists = distributionRepository.findOverlappingDistributions(
                formDto.getAcademicYearId(), startNo, endNo);

        if (!overlappingDists.isEmpty()) {
            handleOverlappingDistributions(overlappingDists, formDto);
        }

        // 3. Create & Map (Pass BOTH types now)
        Distribution distribution = new Distribution();
        mapDtoToDistribution(distribution, formDto, issuedById, issuedToId);
        
        distribution.setIssued_to_emp_id(receiverEmpId);
        distribution.setIssued_to_pro_id(null);

        // 4. Save & Flush
        Distribution savedDist = distributionRepository.saveAndFlush(distribution);

        // 5. Recalculate Balances
        int stateId = savedDist.getState().getStateId();
        Float amount = formDto.getApplication_Amount();

        // A. Update Issuer
        recalculateBalanceForEmployee(issuerUserId, formDto.getAcademicYearId(), stateId, issuedById, issuerUserId, amount);
        
        // B. Update Receiver (Pass the calculated issuedToId)
        addStockToReceiver(savedDist, formDto.getAcademicYearId(), issuedToId, issuerUserId, amount);
    }
	
	@Transactional
    public void updateForm(@NonNull Integer distributionId, @NonNull FormSubmissionDTO formDto) {
        
        // 1. Fetch Existing Record
        Distribution existingDistribution = distributionRepository.findById(distributionId)
                .orElseThrow(() -> new RuntimeException("Distribution record not found with ID: " + distributionId));

        // 2. Extract Immutable Data (Preserve Amount & State!)
        Float originalAmount = existingDistribution.getAmount(); 
        int issuerId = formDto.getUserId();
        int academicYearId = formDto.getAcademicYearId();
        int stateId = existingDistribution.getState().getStateId();

        // 3. Identify Changes
        int oldReceiverId = existingDistribution.getIssued_to_emp_id();
        int newReceiverId = formDto.getDgmEmployeeId();
        boolean isRecipientChanging = oldReceiverId != newReceiverId;

        int oldStart = (int) existingDistribution.getAppStartNo();
        int oldEnd = (int) existingDistribution.getAppEndNo();
        int newStart = Integer.parseInt(formDto.getApplicationNoFrom());
        int newEnd = Integer.parseInt(formDto.getApplicationNoTo());
        boolean isRangeChanging = oldStart != newStart || oldEnd != newEnd;

        // 4. AUTO-DETECT TYPES (Backend Logic)
        // We calculate these from the UserAdminView, we do NOT trust the frontend.
        int issuedById = getRoleTypeIdByEmpId(issuerId);
        int issuedToId = getRoleTypeIdByEmpId(newReceiverId); // <--- Calculated Here

        // -------------------------------------------------------
        // STEP 5: ARCHIVE OLD & SAVE NEW
        // -------------------------------------------------------

        // A. Inactivate Old (Flush to ensure DB sees it as inactive immediately)
        existingDistribution.setIsActive(0);
        distributionRepository.saveAndFlush(existingDistribution);

        // B. Create New Record
        Distribution newDist = new Distribution();
        // Map basic fields (Date, Zone, etc.)
        mapDtoToDistribution(newDist, formDto, issuedById, issuedToId);
        
        // OVERRIDE with correct Logic:
        newDist.setIssued_to_emp_id(newReceiverId); // DGM is always Employee
        newDist.setIssued_to_pro_id(null);
        newDist.setAmount(originalAmount); // CRITICAL: Preserve Original Amount

        distributionRepository.saveAndFlush(newDist); // Flush new record

        // -------------------------------------------------------
        // STEP 6: HANDLE REMAINDERS (If Range Shrank)
        // -------------------------------------------------------
        if (isRangeChanging) {
            // Leftover BEFORE the new range -> Stays with OLD Receiver
            if (oldStart < newStart) {
                createAndSaveRemainder(existingDistribution, oldStart, newStart - 1);
            }
            // Leftover AFTER the new range -> Stays with OLD Receiver
            if (oldEnd > newEnd) {
                createAndSaveRemainder(existingDistribution, newEnd + 1, oldEnd);
            }
        }

        // -------------------------------------------------------
        // STEP 7: RECALCULATE BALANCES
        // -------------------------------------------------------

        // A. Update Issuer (Zone Officer or CO)
        recalculateBalanceForEmployee(issuerId, academicYearId, stateId, issuedById, issuerId, originalAmount);

        // B. Update New Receiver (DGM)
        // We use the calculated 'issuedToId' variable here
        recalculateBalanceForEmployee(
            newReceiverId, 
            academicYearId, 
            stateId, 
            issuedToId, // <--- FIX: Using local variable
            issuerId, 
            originalAmount
        );

        // C. Update Old Receiver (If changed)
        if (isRecipientChanging) {
            // We must find the Type ID of the old receiver to call the method correctly
            // Use LIST check to avoid crashes
            java.util.List<BalanceTrack> oldBalances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(academicYearId, oldReceiverId, originalAmount);
            
            if (!oldBalances.isEmpty()) {
                 BalanceTrack oldBalance = oldBalances.get(0);
                 int oldTypeId = oldBalance.getIssuedByType().getAppIssuedId();
                 
                 recalculateBalanceForEmployee(oldReceiverId, academicYearId, stateId, oldTypeId, issuerId, originalAmount);
            }
        }
    }

	// ---------------------------------------------------------
	// Smart Gap Detection Logic
	// ---------------------------------------------------------
	private void addStockToReceiver(Distribution savedDist, int academicYearId, int typeId, int createdBy, Float amount) {
        int newStart = savedDist.getAppStartNo();
        int newEnd = savedDist.getAppEndNo();
        int newCount = savedDist.getTotalAppCount();
        int targetEnd = newStart - 1;
        int receiverId = savedDist.getIssued_to_emp_id(); // DGM is Employee

        Optional<BalanceTrack> mergeableRow = balanceTrackRepository.findMergeableRowForEmployee(
                academicYearId, receiverId, amount, targetEnd);

        if (mergeableRow.isPresent()) {
            // MERGE
            BalanceTrack existing = mergeableRow.get();
            existing.setAppTo(newEnd);
            existing.setAppAvblCnt(existing.getAppAvblCnt() + newCount);
            balanceTrackRepository.save(existing);
        } else {
            // NEW ROW
            BalanceTrack newRow = createNewBalanceTrack(receiverId, academicYearId, typeId, createdBy);
            newRow.setAmount(amount);
            newRow.setAppFrom(newStart);
            newRow.setAppTo(newEnd);
            newRow.setAppAvblCnt(newCount);
            balanceTrackRepository.save(newRow);
        }
    }

	private BalanceTrack createNewBalanceTrack(int id, int acYear, int typeId, int createdBy, boolean isPro) {
		BalanceTrack nb = new BalanceTrack();
		nb.setAcademicYear(academicYearRepository.findById(acYear).orElseThrow());
		nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());
		nb.setIsActive(1);
		nb.setCreatedBy(createdBy);

		// Strict Validation logic
		if (isPro) {
			nb.setIssuedToProId(id);
			nb.setEmployee(null); // DB allows null now
		} else {
			nb.setEmployee(employeeRepository.findById(id).orElseThrow());
			nb.setIssuedToProId(null);
		}
		return nb;
	}

	private void createAndSaveRemainder(Distribution originalDist, int start, int end) {
        Distribution remainder = new Distribution();
        mapExistingToNewDistribution(remainder, originalDist);
        remainder.setIssued_to_emp_id(originalDist.getIssued_to_emp_id());
        remainder.setAppStartNo(start);
        remainder.setAppEndNo(end);
        remainder.setTotalAppCount((end - start) + 1);
        remainder.setIsActive(1);
        remainder.setAmount(originalDist.getAmount());
        distributionRepository.saveAndFlush(remainder);
    }

	// --- PRIVATE HELPER METHODS ---

	/**
	 * Revised to use inactivation and insertion instead of update/delete.
	 */
	private void handleOverlappingDistributions(List<Distribution> overlappingDists, FormSubmissionDTO request) {
        int newStart = Integer.parseInt(request.getApplicationNoFrom());
        int newEnd = Integer.parseInt(request.getApplicationNoTo());

        for (Distribution oldDist : overlappingDists) {
            int oldReceiverId = oldDist.getIssued_to_emp_id();
            if (oldReceiverId == request.getDgmEmployeeId()) continue;

            // Inactivate
            oldDist.setIsActive(0);
            distributionRepository.saveAndFlush(oldDist); 

            int oldStart = oldDist.getAppStartNo();
            int oldEnd = oldDist.getAppEndNo();
            
            if (oldStart < newStart) createAndSaveRemainder(oldDist, oldStart, newStart - 1);
            if (oldEnd > newEnd) createAndSaveRemainder(oldDist, newEnd + 1, oldEnd);
            
            // Recalculate Balance for Victim
            recalculateBalanceForEmployee(
                oldReceiverId, 
                request.getAcademicYearId(), 
                oldDist.getState().getStateId(),
                oldDist.getIssuedToType().getAppIssuedId(), 
                request.getUserId(),
                oldDist.getAmount()
            );
        }
    }

	// 2. Recalculate Logic (Updated for Amount & AdminApp)
private void recalculateBalanceForEmployee(int employeeId, int academicYearId, int stateId, int typeId, int createdBy, Float amount) {
        
        // 1. CHECK: Is this a CO/Admin? (Check Master Table)
        Optional<AdminApp> adminApp = adminAppRepository.findByEmpAndYearAndAmount(
                employeeId, academicYearId, amount);

        if (adminApp.isPresent()) {
            // --- CASE A: CO / ADMIN (The Source) ---
            AdminApp master = adminApp.get();
            
            // FIX: Handle LIST return type
            List<BalanceTrack> balances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(
                    academicYearId, employeeId, amount);
            
            BalanceTrack balance;
            if (balances.isEmpty()) {
                balance = createNewBalanceTrack(employeeId, academicYearId, typeId, createdBy);
                balance.setAmount(amount);
            } else {
                // Admins act as a single bucket, so we pick the first one
                balance = balances.get(0); 
            }

            int totalDistributed = distributionRepository.sumTotalAppCountByCreatedByAndAmount(
                    employeeId, academicYearId, amount).orElse(0);
            
            balance.setAppFrom(master.getAppFromNo());
            balance.setAppTo(master.getAppToNo());
            balance.setAppAvblCnt(master.getTotalApp() - totalDistributed);
            
            balanceTrackRepository.save(balance);
        
        } else {
            // --- CASE B: INTERMEDIARIES (Zone/DGM) ---
            // Rebuilds rows to match gaps exactly
        	rebuildBalancesFromDistributions(
        	        employeeId, 
        	        academicYearId, 
        	        typeId, 
        	        createdBy, 
        	        amount // <--- Ensure you convert Float to Double here
        	    );
        }
    }

//--- HELPER: Rebuild Balance Rows (Preserves Gaps) ---
// This deletes old balance rows and creates new ones based on what is currently held in Distribution table
private void rebuildBalancesFromDistributions(int empId, int acYearId, int typeId, int createdBy, Float amount) {
    
    // 1. Get ALL Active Distributions currently HELD by this user
    List<Distribution> holdings = distributionRepository.findActiveByIssuedToEmpIdAndAmountOrderByStart(
            empId, acYearId, amount);

    // 2. Get CURRENT Active Balance Rows for this amount
    List<BalanceTrack> currentBalances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(
            acYearId, empId, amount);

    // 3. STRATEGY: Soft Delete OLD rows, Insert NEW rows
    
    // A. Mark old rows as inactive (Clear the slate)
    for (BalanceTrack b : currentBalances) {
        b.setIsActive(0);
        balanceTrackRepository.save(b);
    }
    
    // B. Create new rows for every active distribution held (Mirroring reality)
    for (Distribution dist : holdings) {
        BalanceTrack nb = createNewBalanceTrack(empId, acYearId, typeId, createdBy);
        
        nb.setAmount(amount);
        nb.setAppFrom((int) dist.getAppStartNo());
        nb.setAppTo((int) dist.getAppEndNo());
        nb.setAppAvblCnt(dist.getTotalAppCount());
        
        balanceTrackRepository.save(nb);
    }
}
	/**
	 * Helper to create a new active Distribution record based on an existing one
	 * but setting the new IssuedToEmpId (the old receiver) and setting IsActive=1.
	 */
	private Distribution createRemainderDistribution(Distribution originalDist, int receiverId) {
		Distribution remainderDistribution = new Distribution();
		// Copy most fields from the original distribution
		mapExistingToNewDistribution(remainderDistribution, originalDist);

		// Set specific fields for the remainder
		remainderDistribution.setIssued_to_emp_id(receiverId); // Stays with the OLD receiver
		remainderDistribution.setIsActive(1);

		// Note: The range and count will be set by the caller
		return remainderDistribution;
	}

	// FIX: Added 4th parameter 'int issuedToId'
    private void mapDtoToDistribution(Distribution distribution, FormSubmissionDTO formDto, int issuedById, int issuedToId) {
        
        int appNoFrom = Integer.parseInt(formDto.getApplicationNoFrom());
        int appNoTo = Integer.parseInt(formDto.getApplicationNoTo());

        // Map Basic Fields
        academicYearRepository.findById(formDto.getAcademicYearId()).ifPresent(distribution::setAcademicYear);
        zoneRepository.findById(formDto.getZoneId()).ifPresent(distribution::setZone);
        campusRepository.findById(formDto.getCampusId()).ifPresent(distribution::setCampus);
        
        cityRepository.findById(formDto.getCityId()).ifPresent(city -> {
            distribution.setCity(city);
            if (city.getDistrict() != null) {
                distribution.setDistrict(city.getDistrict());
                if (city.getDistrict().getState() != null) {
                    distribution.setState(city.getDistrict().getState()); 
                }
            }
        });

        // --- FIX: Use the passed arguments for Types ---
        appIssuedTypeRepository.findById(issuedById).ifPresent(distribution::setIssuedByType);
        appIssuedTypeRepository.findById(issuedToId).ifPresent(distribution::setIssuedToType);
        // ----------------------------------------------

        distribution.setAppStartNo(appNoFrom);
        distribution.setAppEndNo(appNoTo);
        distribution.setTotalAppCount(formDto.getRange());
        distribution.setAmount(formDto.getApplication_Amount());
        
        // Fix for Date (Always use Now to prevent nulls)
        distribution.setIssueDate(java.time.LocalDateTime.now());
        
        distribution.setIsActive(1);
        distribution.setCreated_by(formDto.getUserId());
        
        // Note: We set issued_to_emp_id in the main method, not here.
    }

	private void mapExistingToNewDistribution(Distribution newDist, Distribution oldDist) {
	       // ... (Copy standard fields) ...
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
        nb.setEmployee(employeeRepository.findById(employeeId).orElseThrow());
        nb.setAcademicYear(academicYearRepository.findById(academicYearId).orElseThrow());
        nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());
        nb.setAppAvblCnt(0);
        nb.setIsActive(1);
        nb.setCreatedBy(createdBy);
        nb.setIssuedToProId(null); // Strict Validation for Employee
        return nb;
    }

}
