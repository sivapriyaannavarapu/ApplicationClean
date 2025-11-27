package com.application.service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.application.dto.DistributionRequestDTO;
import com.application.dto.EmployeesDto;
import com.application.entity.AcademicYear;
import com.application.entity.AdminApp;
import com.application.entity.BalanceTrack;
import com.application.entity.City;
import com.application.entity.Distribution;
import com.application.entity.State;
import com.application.entity.ZonalAccountant;
import com.application.entity.Zone;
import com.application.repository.AcademicYearRepository;
import com.application.repository.AdminAppRepository;
import com.application.repository.AppIssuedTypeRepository;
import com.application.repository.BalanceTrackRepository;
import com.application.repository.CampusProViewRepository;
import com.application.repository.CityRepository;
import com.application.repository.DistributionRepository;
import com.application.repository.EmployeeRepository;
import com.application.repository.StateRepository;
import com.application.repository.ZonalAccountantRepository;
import com.application.repository.ZoneRepository;

import lombok.NonNull;

@Service
public class ZoneService {

	private final AcademicYearRepository academicYearRepository;
	private final StateRepository stateRepository;
	private final CityRepository cityRepository;
	private final ZoneRepository zoneRepository;
	private final AppIssuedTypeRepository appIssuedTypeRepository;
	private final EmployeeRepository employeeRepository;
	private final BalanceTrackRepository balanceTrackRepository;
	private final DistributionRepository distributionRepository;
	private final ZonalAccountantRepository zonalAccountantRepository;
	@Autowired
	private AdminAppRepository adminAppRepository;
	@Autowired
	private CampusProViewRepository campusProViewRepository;

	public ZoneService(AcademicYearRepository academicYearRepository, StateRepository stateRepository,
			CityRepository cityRepository, ZoneRepository zoneRepository,
			AppIssuedTypeRepository appIssuedTypeRepository, EmployeeRepository employeeRepository,
			BalanceTrackRepository balanceTrackRepository,
			DistributionRepository distributionRepository, ZonalAccountantRepository zonalAccountantRepository) {
		this.academicYearRepository = academicYearRepository;
		this.stateRepository = stateRepository;
		this.cityRepository = cityRepository;
		this.zoneRepository = zoneRepository;
		this.appIssuedTypeRepository = appIssuedTypeRepository;
		this.employeeRepository = employeeRepository;
		this.balanceTrackRepository = balanceTrackRepository;
		this.distributionRepository = distributionRepository;
		this.zonalAccountantRepository = zonalAccountantRepository;
	}

	// --- Dropdown/Helper Methods with Caching ---
	@Cacheable("academicYears")
	public List<AcademicYear> getAllAcademicYears() {
		return academicYearRepository.findAll();
	}

	@Cacheable("states")
	public List<State> getAllStates() {
		// Assumption: State entity has a field named 'is_active' or similar.
		return stateRepository.findByStatus(1);
	}

	@Cacheable(cacheNames = "citiesByState", key = "#stateId")
	public List<City> getCitiesByState(int stateId) {
		final int ACTIVE_STATUS = 1;
		return cityRepository.findByDistrictStateStateIdAndStatus(stateId, ACTIVE_STATUS);
	}

	@Cacheable(cacheNames = "zonesByCity", key = "#cityId")
	public List<Zone> getZonesByCity(int cityId) {
		return zoneRepository.findByCityCityId(cityId);
	}

	@Cacheable(cacheNames = "employeesByZone", key = "#zoneId")
	@Transactional(readOnly = true) // Recommended for lazy loading
	public List<EmployeesDto> getEmployeesByZone(int zoneId) {
		// Fetch only active zonal accountants (ZonalAccountant.isActive == 1)
		List<ZonalAccountant> activeAccountants = zonalAccountantRepository.findByZoneZoneIdAndIsActive(zoneId, 1);

		// Map and filter: Only include if Employee.isActive == 1
		return activeAccountants.stream().map(this::mapToEmployeeDto).filter(Objects::nonNull) // Skip null/inactive
																								// employees
				.collect(Collectors.toList());
	}

	// Helper: Maps only if employee is active (filters but doesn't include in DTO)
	private EmployeesDto mapToEmployeeDto(ZonalAccountant accountant) {
		var employee = accountant.getEmployee();
		// Filter: Skip if employee null or inactive
		if (employee == null || employee.getIsActive() == null || employee.getIsActive() != 1) {
			return null;
		}
		// Map without isActive
		return new EmployeesDto(employee.getEmp_id(), employee.getFirst_name(), employee.getLast_name(),
				employee.getPrimary_mobile_no());
	}

	@Transactional
	public void saveDistribution(@NonNull DistributionRequestDTO request) {

		validateEmployeeExists(request.getCreatedBy(), "Issuer");

		List<Distribution> overlappingDists = distributionRepository.findOverlappingDistributions(
				request.getAcademicYearId(), request.getAppStartNo(), request.getAppEndNo());

		if (!overlappingDists.isEmpty()) {
			handleOverlappingDistributions(overlappingDists, request);
		}

		ZonalAccountant receiver = zonalAccountantRepository.findByEmployeeEmpId(request.getIssuedToEmpId())
				.orElseThrow(() -> new RuntimeException(
						"Receiver not found for Employee ID: " + request.getIssuedToEmpId()));

		// Optional: You can log a warning if the Zone doesn't match what the UI sent
		if (receiver.getZone().getZoneId() != request.getZoneId()) {
			System.out.println("WARNING: UI sent Zone " + request.getZoneId() + " but User "
					+ request.getIssuedToEmpId() + " is actually in Zone " + receiver.getZone().getZoneId());
		}

		if (receiver.getIsActive() != 1) {
			throw new RuntimeException("Transaction Failed: The selected Receiver is Inactive.");
		}

		Distribution newDistribution = new Distribution();
		mapDtoToDistribution(newDistribution, request); // Helper to map basic fields (State, Zone, Dates, etc.)

		// LOGIC: Determine where to save the ID (Employee Column vs PRO Column)
		if (receiver.getEmployee() != null) {
			// It's an Employee (e.g., DGM)
			newDistribution.setIssued_to_emp_id(receiver.getEmployee().getEmp_id());
			newDistribution.setIssued_to_pro_id(null);
		} else if (receiver.getCampus() != null) {
			// It's a PRO (Branch)
			newDistribution.setIssued_to_pro_id(receiver.getCampus().getCampusId());
			newDistribution.setIssued_to_emp_id(null);
		} else {
			throw new RuntimeException("Invalid Receiver: No Employee or Campus linked to this Zonal Accountant.");
		}

		// Set Fee/Amount
		newDistribution.setAmount(request.getApplication_Amount());

		// Save to DB
		Distribution savedDist = distributionRepository.saveAndFlush(newDistribution);

		recalculateBalanceForEmployee(savedDist.getIssued_to_emp_id(), request.getAcademicYearId(),
				request.getStateId(), request.getIssuedToTypeId(), request.getCreatedBy(),
				request.getApplication_Amount());

		// B. Update Receiver's Balance
		if (savedDist.getIssued_to_emp_id() != null) {
			addStockToReceiver(savedDist.getIssued_to_emp_id(), request.getAcademicYearId(),
					request.getIssuedToTypeId(), request.getCreatedBy(), request.getApplication_Amount(),
					request.getAppStartNo(), // Pass Start
					request.getAppEndNo(), // Pass End
					request.getRange() // Pass Count
			);
		}
	}

	@Transactional
	public void updateDistribution(int distributionId, @NonNull DistributionRequestDTO request) {

		// 1. Fetch Existing
		validateEmployeeExists(request.getCreatedBy(), "Issuer");
		Distribution existingDist = distributionRepository.findById(distributionId)
				.orElseThrow(() -> new RuntimeException("Record not found"));

		Float originalAmount = existingDist.getAmount(); // Keep amount!

		// 2. Resolve New Receiver (Zone Service always targets Employee column)
		// (Assuming you updated your Repo to use findByEmployeeEmpId)
		ZonalAccountant newReceiver = zonalAccountantRepository.findByEmployeeEmpId(request.getIssuedToEmpId())
				.orElseThrow(() -> new RuntimeException("New Receiver not found"));

		// Determine Target ID (Logic: always grab the emp_id, even for campuses)
		Integer newTargetId;
		if (newReceiver.getEmployee() != null) {
			newTargetId = newReceiver.getEmployee().getEmp_id();
		} else {
			newTargetId = campusProViewRepository.findEmployeeIdsByCampusId(newReceiver.getCampus().getCampusId())
					.stream().findFirst().orElseThrow(() -> new RuntimeException("No valid ID found for Campus"));
		}

		// 3. Inactivate Old
		existingDist.setIsActive(0);
		distributionRepository.saveAndFlush(existingDist);

		// 4. Create New
		Distribution newDist = new Distribution();
		mapDtoToDistribution(newDist, request);

		newDist.setIssued_to_emp_id(newTargetId);
		newDist.setIssued_to_pro_id(null); // Zone service keeps this null
		newDist.setAmount(originalAmount); // Preserve Amount

		distributionRepository.saveAndFlush(newDist);

		// 5. Handle Remainders
		int oldStart = (int) existingDist.getAppStartNo();
		int oldEnd = (int) existingDist.getAppEndNo();

		if (oldStart != request.getAppStartNo() || oldEnd != request.getAppEndNo()) {
			if (oldStart < request.getAppStartNo()) {
				createAndSaveRemainder(existingDist, oldStart, request.getAppStartNo() - 1);
			}
			if (oldEnd > request.getAppEndNo()) {
				createAndSaveRemainder(existingDist, request.getAppEndNo() + 1, oldEnd);
			}
		}

		// 6. Recalculate Balances
		int acYear = existingDist.getAcademicYear().getAcdcYearId();
		int stateId = existingDist.getState().getStateId();

		// A. Issuer
		recalculateBalanceForEmployee(request.getCreatedBy(), acYear, stateId, request.getIssuedByTypeId(),
				request.getCreatedBy(), originalAmount);

		// B. New Receiver
		recalculateBalanceForEmployee(newTargetId, acYear, stateId, request.getIssuedToTypeId(), request.getCreatedBy(),
				originalAmount);

		// C. Old Receiver (If changed)
		Integer oldId = existingDist.getIssued_to_emp_id();
		if (oldId != null && (!Objects.equals(oldId, newTargetId)
				|| (oldStart != request.getAppStartNo() || oldEnd != request.getAppEndNo()))) {
			recalculateBalanceForEmployee(oldId, acYear, stateId, existingDist.getIssuedToType().getAppIssuedId(),
					request.getCreatedBy(), originalAmount);
		}
	}

	private void createAndSaveRemainder(Distribution originalDist, int start, int end) {
		Distribution remainder = new Distribution();

		// Copy standard fields
		remainder.setAcademicYear(originalDist.getAcademicYear());
		remainder.setState(originalDist.getState());
		remainder.setCity(originalDist.getCity());
		remainder.setZone(originalDist.getZone());
		remainder.setDistrict(originalDist.getDistrict());
		remainder.setIssuedByType(originalDist.getIssuedByType());
		remainder.setIssuedToType(originalDist.getIssuedToType());
		remainder.setCreated_by(originalDist.getCreated_by());
		remainder.setIssueDate(originalDist.getIssueDate() != null ? originalDist.getIssueDate() : LocalDateTime.now());
		remainder.setAmount(originalDist.getAmount());
		remainder.setIssued_to_emp_id(originalDist.getIssued_to_emp_id());
		remainder.setIssued_to_pro_id(originalDist.getIssued_to_pro_id());

		// Set New Range
		remainder.setAppStartNo(start);
		remainder.setAppEndNo(end);
		remainder.setTotalAppCount((end - start) + 1);
		remainder.setIsActive(1); // Active

		distributionRepository.saveAndFlush(remainder);
	}

	private void handleOverlappingDistributions(List<Distribution> overlappingDists, DistributionRequestDTO request) {
		int reqStart = request.getAppStartNo();
		int reqEnd = request.getAppEndNo();

		for (Distribution oldDist : overlappingDists) {

			// 1. Identify the "Old Holder" (The Victim)
			Integer oldHolderId;
			boolean isPro = false;

			if (oldDist.getIssued_to_pro_id() != null) {
				oldHolderId = oldDist.getIssued_to_pro_id();
				isPro = true;
			} else if (oldDist.getIssued_to_emp_id() != null) {
				oldHolderId = oldDist.getIssued_to_emp_id();
				isPro = false;
			} else {
				continue;
			}

			// [DELETED] The check that skipped self-updates is gone.
			// We process EVERY overlap to ensure the old record is deactivated.

			// 2. Inactivate OLD (Soft Delete)
			oldDist.setIsActive(0);

			// CRITICAL: Flush so the database knows this is now 0
			distributionRepository.saveAndFlush(oldDist);

			int oldStart = oldDist.getAppStartNo();
			int oldEnd = oldDist.getAppEndNo();

			// 3. Create "Before" Split (Remainder)
			if (oldStart < reqStart) {
				createAndSaveRemainder(oldDist, oldStart, reqStart - 1);
			}

			// 4. Create "After" Split (Remainder)
			if (oldEnd > reqEnd) {
				createAndSaveRemainder(oldDist, reqEnd + 1, oldEnd);
			}

			// 5. Recalculate Balance for the OLD HOLDER (The Victim)
			// We use the Old Distribution's metadata (State, Type, Amount)
			int acYear = request.getAcademicYearId();
			int stateId = oldDist.getState().getStateId();
			int typeId = oldDist.getIssuedToType().getAppIssuedId();
			int modifierId = request.getCreatedBy();
			Float amount = oldDist.getAmount(); // Keep Original Amount

			recalculateBalanceForEmployee(oldHolderId, acYear, stateId, typeId, modifierId, amount);
		}
	}

	private void recalculateBalanceForEmployee(int employeeId, int academicYearId, int stateId, int typeId,
			int createdBy, Float amount) {

		// 1. CHECK: Is this a CO/Admin? (Check Master Table)
		// We convert Float to Float for the repo call
		Optional<AdminApp> adminApp = adminAppRepository.findByEmpAndYearAndAmount(employeeId, academicYearId, amount);

		if (adminApp.isPresent()) {
			// --- CASE A: CO / ADMIN (The Source) ---
			// Logic: Master Allocation - Total Distributed

			AdminApp master = adminApp.get();

			// Admins usually have 1 giant balance row, so we fetch/create just one.
			List<BalanceTrack> balances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(academicYearId,
					employeeId, amount);

			BalanceTrack balance;
			if (balances.isEmpty()) {
				balance = createNewBalanceTrack(employeeId, academicYearId, typeId, createdBy);
				balance.setAmount(amount);
			} else {
				balance = balances.get(0); // Use the existing one
			}

			// Calculate Total Distributed by Admin
			int totalDistributed = distributionRepository
					.sumTotalAppCountByCreatedByAndAmount(employeeId, academicYearId, amount).orElse(0);

			// Update Logic
			balance.setAppFrom(master.getAppFromNo()); // Start at Master Start
			balance.setAppTo(master.getAppToNo());
			balance.setAppAvblCnt(master.getTotalApp() - totalDistributed);

			balanceTrackRepository.save(balance);

		} else {
			// --- CASE B: ZONE & DGM (The Intermediaries) ---
			// They do NOT have a master table. They rely purely on what they HOLD.
			// We call the helper to rebuild their balance rows to match their holdings.

			rebuildBalancesFromDistributions(employeeId, academicYearId, typeId, createdBy, amount);
		}
	}

	private void rebuildBalancesFromDistributions(int empId, int acYearId, int typeId, int createdBy, Float amount) {

		// 1. Get ALL Active Distributions currently HELD by this user
		// (Ordered by Start Number so it looks nice)
		List<Distribution> holdings = distributionRepository.findActiveByIssuedToEmpIdAndAmountOrderByStart(empId,
				acYearId, amount);

		// 2. Get CURRENT Active Balance Rows for this amount
		List<BalanceTrack> currentBalances = balanceTrackRepository.findActiveBalancesByEmpAndAmount(acYearId, empId,
				amount);

		// 3. STRATEGY: Soft Delete OLD rows, Insert NEW rows
		// This effectively "Refreshes" the balance to match reality.

		// A. Mark old rows as inactive
		for (BalanceTrack b : currentBalances) {
			b.setIsActive(0);
			balanceTrackRepository.save(b);
		}

		// B. Create new rows for every active distribution held
		for (Distribution dist : holdings) {
			BalanceTrack nb = createNewBalanceTrack(empId, acYearId, typeId, createdBy);

			nb.setAmount(amount);
			nb.setAppFrom((int) dist.getAppStartNo());
			nb.setAppTo((int) dist.getAppEndNo());
			nb.setAppAvblCnt(dist.getTotalAppCount());

			balanceTrackRepository.save(nb);
		}
	}

	private Distribution createRemainderDistribution(Distribution originalDist, int receiverId) {
		Distribution remainderDistribution = new Distribution();
		// Copy most fields from the original distribution
		mapDtoToDistribution(remainderDistribution, createDtoFromDistribution(originalDist));

		// Set specific fields for the remainder
		remainderDistribution.setIssued_to_emp_id(receiverId); // Stays with the OLD receiver
		remainderDistribution.setIsActive(1);

		// Note: The range and count will be set by the caller (updateDistribution)
		return remainderDistribution;
	}

	private void mapDtoToDistribution(Distribution d, DistributionRequestDTO req) {
		d.setAcademicYear(academicYearRepository.findById(req.getAcademicYearId()).orElseThrow());
		d.setState(stateRepository.findById(req.getStateId()).orElseThrow());
		d.setZone(zoneRepository.findById(req.getZoneId()).orElseThrow());
		d.setIssuedByType(appIssuedTypeRepository.findById(req.getIssuedByTypeId()).orElseThrow());
		d.setIssuedToType(appIssuedTypeRepository.findById(req.getIssuedToTypeId()).orElseThrow());
		City city = cityRepository.findById(req.getCityId()).orElseThrow();
		d.setCity(city);
		d.setDistrict(city.getDistrict());
		d.setAmount(req.getApplication_Amount());
		d.setIssueDate(LocalDateTime.now());
		d.setIssued_to_emp_id(req.getIssuedToEmpId());
		d.setCreated_by(req.getCreatedBy());
		d.setAppStartNo(req.getAppStartNo());
		d.setAppEndNo(req.getAppEndNo());
		d.setTotalAppCount(req.getRange());
		d.setIsActive(1);
	}

	private DistributionRequestDTO createDtoFromDistribution(Distribution dist) {
		DistributionRequestDTO dto = new DistributionRequestDTO();
		dto.setAcademicYearId(dist.getAcademicYear().getAcdcYearId());
		dto.setStateId(dist.getState().getStateId());
		dto.setCityId(dist.getCity().getCityId());
		dto.setZoneId(dist.getZone().getZoneId());
		dto.setIssuedByTypeId(dist.getIssuedByType().getAppIssuedId());
		dto.setIssuedToTypeId(dist.getIssuedToType().getAppIssuedId());
		dto.setIssuedToEmpId(dist.getIssued_to_emp_id());
		dto.setApplication_Amount(dist.getAmount());
		dto.setAppStartNo(dist.getAppStartNo());
		dto.setAppEndNo(dist.getAppEndNo());
		dto.setRange(dist.getTotalAppCount());
//		dto.setIssueDate(dist.getIssueDate());
		dto.setCreatedBy(dist.getCreated_by());
		return dto;
	}

	private BalanceTrack createNewBalanceTrack(int employeeId, int academicYearId, int typeId, int createdBy) {
		BalanceTrack nb = new BalanceTrack();
		nb.setEmployee(employeeRepository.findById(employeeId).orElseThrow());
		nb.setAcademicYear(academicYearRepository.findById(academicYearId).orElseThrow());
		nb.setIssuedByType(appIssuedTypeRepository.findById(typeId).orElseThrow());

		nb.setIssuedToProId(null); // Strict Validation: It's an Employee
		nb.setAppAvblCnt(0);
		nb.setIsActive(1);
		nb.setCreatedBy(createdBy);
		return nb;
	}

	private void validateEmployeeExists(int employeeId, String role) {
		if (employeeId <= 0 || !employeeRepository.existsById(employeeId)) {
			throw new IllegalArgumentException(role + " employee not found or invalid ID: " + employeeId);
		}
	}

	// This is SPECIFICALLY for adding new stock to a Receiver (Zone/DGM)
	private void addStockToReceiver(int employeeId, int academicYearId, int typeId, int createdBy, Float amount,
			int newStart, int newEnd, int newCount) {

		// 1. Calculate the "Target End" (The number immediately before the new batch)
		int targetEnd = newStart - 1;

		// 2. Check if we can MERGE with an existing row
		Optional<BalanceTrack> mergeableRow = balanceTrackRepository.findMergeableRowForEmployee(academicYearId,
				employeeId, amount, targetEnd);

		if (mergeableRow.isPresent()) {
			// SCENARIO: CONTIGUOUS (1-50 exists, adding 51-100)
			BalanceTrack existing = mergeableRow.get();

			// Update the existing row
			existing.setAppTo(newEnd); // Extend the range (50 -> 100)
			existing.setAppAvblCnt(existing.getAppAvblCnt() + newCount); // Add count

			balanceTrackRepository.save(existing);
		} else {
			// SCENARIO: DISTURBED / GAP (1-50 exists, adding 101-150)
			// Create a BRAND NEW row
			BalanceTrack newRow = createNewBalanceTrack(employeeId, academicYearId, typeId, createdBy);
			newRow.setEmployee(employeeRepository.findById(employeeId).orElseThrow());
			newRow.setIssuedToProId(null);
			newRow.setAmount(amount);
			newRow.setIsActive(1);

			// Set the specific range for this packet
			newRow.setAppFrom(newStart);
			newRow.setAppTo(newEnd);
			newRow.setAppAvblCnt(newCount);

			balanceTrackRepository.save(newRow);
		}
	}
}