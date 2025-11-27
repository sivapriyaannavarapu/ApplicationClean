package com.application.service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.application.dto.AppStatusDetailsDTO;
import com.application.dto.AppStatusResponseDTO;
import com.application.dto.ApplicationDamagedDto;
import com.application.dto.Appstatusdtodamaged;
import com.application.dto.CampusDto;
import com.application.dto.EmployeeDto;
import com.application.dto.GenericDropdownDTO;
import com.application.entity.AppStatus;
import com.application.entity.AppStatusTrackView;
import com.application.entity.ApplicationStatus;
import com.application.entity.Campus;
import com.application.entity.CampusProView;
import com.application.entity.Dgm;
import com.application.entity.Employee;
import com.application.entity.Status;
import com.application.entity.StudentAcademicDetails;
import com.application.entity.Zone;
import com.application.exception.ApplicationAlreadyExistsException;
import com.application.repository.AppStatusRepository;
import com.application.repository.AppStatusTrackViewRepository;
import com.application.repository.ApplicationStatusRepository;
import com.application.repository.CampusProViewRepository;
import com.application.repository.CampusRepository;
import com.application.repository.DgmRepository;
import com.application.repository.EmployeeRepository;
import com.application.repository.StatusRepository;
import com.application.repository.StudentAcademicDetailsRepository;
import com.application.repository.ZoneRepository;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.PersistenceContext;

@Service
public class ApplicationDamagedService {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired private AppStatusRepository appStatusRepository;
    @Autowired private StatusRepository statusRepository;
    @Autowired private EmployeeRepository employeeRepository;
    @Autowired private CampusRepository campusRepository;
    @Autowired private CampusProViewRepository campusProViewRepository;
    @Autowired private AppStatusTrackViewRepository appStatusTrackViewRepository;
    @Autowired public ApplicationStatusRepository applicationStatusRepository;
    @Autowired public DgmRepository dgmRepository;
    @Autowired public ZoneRepository zoneRepository;
    @Autowired private StudentAcademicDetailsRepository studentAcademicDetailsRepository;

    // ---------------------- READ METHODS (CACHEABLE) ----------------------

    @Cacheable(value = "appStatusTrackView", key = "#num")
    public AppStatusTrackView getEntityByApplicationNo(int num) {
        return appStatusTrackViewRepository.findById(num).orElse(null);
    }

    // Return DTO to Controller
    public Appstatusdtodamaged getDetailsByApplicationNo(int num) {

        AppStatusTrackView view = getEntityByApplicationNo(num);

        if (view == null) {
            return null;
        }

        return mapToDTO(view);
    }

    private Appstatusdtodamaged mapToDTO(AppStatusTrackView view) {
    	Appstatusdtodamaged dto = new Appstatusdtodamaged();

        dto.setApplicationNo(view.getNum());
        dto.setProEmpId(view.getPro_emp_id());
        dto.setProName(view.getPro_name());
        dto.setDgmEmpId(view.getDgm_emp_id());
        dto.setDgmName(view.getDgm_name());
        dto.setZoneId(view.getZone_id());
        dto.setZoneName(view.getZone_name());
        dto.setCampusId(view.getCmps_id());
        dto.setCampusName(view.getCmps_name());
        dto.setStatus(view.getStatus());
        dto.setStatusDate(view.getDate());

        return dto;
    }

    @Cacheable(value = "zoneEmployees")
    public List<Employee> getAllZoneEmployees() {
        return employeeRepository.findAll();
    }

    @Cacheable(value = "allCampuses")
    public List<GenericDropdownDTO> getActiveCampusesDropdown() {
        return campusRepository.findAllActiveCampusesForDropdown();
    }

    @Cacheable(value = "dgmCampuses")
    public List<GenericDropdownDTO> getDgmCampusesDropdown() {
        return dgmRepository.findDistinctActiveCampusesByDgm();
    }

    @Cacheable(value = "allStatuses")
    public List<ApplicationStatus> getAllStatus() {
        return applicationStatusRepository.findByIsActive(1);
    }


    @Cacheable(value = "zonesDropdown")
    public List<GenericDropdownDTO> getAllZones() {
        return zoneRepository.findAllActiveZonesForDropdown();
    }

    @Cacheable(value = "campusesByDgm", key = "#dgmId")
    public List<Campus> getCampusesByDgmId(int dgmId) {
        List<Dgm> dgmEntries = dgmRepository.findByDgmId(dgmId);
        if (dgmEntries.isEmpty()) return List.of();
        return dgmEntries.stream().map(Dgm::getCampus).collect(Collectors.toList());
    }

    @Cacheable(value = "appStatusDetails", key = "#appNo")
    public Optional<AppStatusDetailsDTO> getAppStatusDetails(int appNo) {
        Optional<AppStatus> appStatusOptional = appStatusRepository.findByApplicationNumber(appNo);
        if (appStatusOptional.isEmpty()) return Optional.empty();

        AppStatus appStatus = appStatusOptional.get();
        AppStatusDetailsDTO dto = new AppStatusDetailsDTO();

        if (appStatus.getEmployee() != null) {
            dto.setProId(appStatus.getEmployee().getEmp_id());
            dto.setProName(appStatus.getEmployee().getFirst_name() + " " + appStatus.getEmployee().getLast_name());
        }
        if (appStatus.getZone() != null) {
            dto.setZoneId(appStatus.getZone().getZoneId());
            dto.setZoneName(appStatus.getZone().getZoneName());
        }
        if (appStatus.getEmployee2() != null) {
            dto.setDgmEmpId(appStatus.getEmployee2().getEmp_id());
            dto.setDgmEmpName(appStatus.getEmployee2().getFirst_name() + " " + appStatus.getEmployee2().getLast_name());
        }
        if (appStatus.getCampus() != null) {
            dto.setCampusId(appStatus.getCampus().getCampusId());
            dto.setCampusName(appStatus.getCampus().getCampusName());
        }
        if (appStatus.getStatus() != null) {
            dto.setStatus(appStatus.getStatus().getStatus_type());
        }

        dto.setReason(appStatus.getReason());

        return Optional.of(dto);
    }

    @Cacheable(value = "campusesByZone", key = "#zoneId")
    public List<CampusDto> getCampusDtosByZoneId(int zoneId) {
        Optional<Zone> zoneOptional = zoneRepository.findById(zoneId);
        return zoneOptional.map(zone -> campusRepository.findByZoneAsDto(zone))
                           .orElse(List.of());
    }

    // ---------------------- WRITE METHODS ----------------------

  public AppStatusResponseDTO  saveOrUpdateApplicationStatus(ApplicationDamagedDto dto) {
 
        if (dto == null)
            throw new IllegalArgumentException("DTO cannot be null");
 
        Long appNo = dto.getApplicationNo().longValue();
 
        // 1️⃣ Check if application already used (Exists in Student table)
        Optional<StudentAcademicDetails> studentOpt =
                studentAcademicDetailsRepository.findByStudAdmsNo(appNo);
 
        if (studentOpt.isPresent()) {
            String studStatus = studentOpt.get().getStatus().getStatus_type();
            throw new ApplicationAlreadyExistsException(
                    "Application already USED with status: " + studStatus
            );
        }
 
        // 2️⃣ Check if already exists in app_status
        Optional<AppStatus> existingAppStatusOpt =
                appStatusRepository.findByApp_no(dto.getApplicationNo());
 
        AppStatus appStatus;
 
        if (existingAppStatusOpt.isPresent()) {
            // If exists → check whether at PRO stage
            appStatus = existingAppStatusOpt.get();
 
            if (appStatus.getStatus().getStatus_id() != 1) { // 1 = PRO
                throw new IllegalStateException(
                        "Application is not with PRO. Current status: " +
                        appStatus.getStatus().getStatus_type()
                );
            }
 
            // Allowed to damage → Updating existing record
        } else {
            // Create a new damage record
            appStatus = new AppStatus();
            appStatus.setApp_no(dto.getApplicationNo());
            appStatus.setCreated_by(2);
        }
 
        // 3️⃣ Fetch reference objects
        Status status = statusRepository.findById(dto.getStatusId())
                .orElseThrow(() -> new EntityNotFoundException("Status not found"));
 
        Campus campus = campusRepository.findById(dto.getCampusId())
                .orElseThrow(() -> new EntityNotFoundException("Campus not found"));
 
        Employee proEmployee = employeeRepository.findById(dto.getProId())
                .orElseThrow(() -> new EntityNotFoundException("PRO Employee not found"));
 
        Zone zone = zoneRepository.findById(dto.getZoneId())
                .orElseThrow(() -> new EntityNotFoundException("Zone not found"));
 
        Employee dgmEmployee = employeeRepository.findById(dto.getDgmEmpId())
                .orElseThrow(() -> new EntityNotFoundException("DGM Employee not found"));
 
        // 4️⃣ Set values
        appStatus.setReason(dto.getReason());
        appStatus.setStatus(status); // here status = DAMAGED
        appStatus.setCampus(campus);
        appStatus.setEmployee(proEmployee);
        appStatus.setZone(zone);
        appStatus.setEmployee2(dgmEmployee);
        if (dto.getStatusId() == 3) {        // AVAILABLE
            appStatus.setIs_active(0);
        } else {
            appStatus.setIs_active(1);        // DAMAGED etc.
        }
        appStatus.setUpdated_date(LocalDate.now());
 
        // 5️⃣ Save
        AppStatus saved = appStatusRepository.save(appStatus);
 
        return convertToDTO(saved);
    }
    private AppStatusResponseDTO convertToDTO(AppStatus entity) {
    	
 
    	ApplicationStatus status = applicationStatusRepository.findById(entity.getStatus().getStatus_id())
                .orElse(null);
        AppStatusResponseDTO dto = new AppStatusResponseDTO();
 
        dto.setAppNo(entity.getApp_no());
        dto.setStatus(status != null ? status.getStatus() : null);
        dto.setCampusName(entity.getCampus().getCampusName());
        dto.setProName(entity.getEmployee().getFirst_name());
        dto.setZoneName(entity.getZone().getZoneName());
        dto.setDgmName(entity.getEmployee2().getFirst_name());
        dto.setReason(entity.getReason());
        dto.setUpdatedDate(entity.getUpdated_date().toString());
 
        return dto;
    }
 

    // ---------------------- SUPPORT METHODS ----------------------

    @Cacheable(value = "dgmNamesByZone", key = "#zoneId")
    public List<EmployeeDto> getDgmNamesByZoneId(int zoneId) {
        List<Dgm> dgmList = dgmRepository.findByZoneId(zoneId);
        List<EmployeeDto> dgmEmployees = new ArrayList<>();

        for (Dgm dgm : dgmList) {
            if (dgm.getEmployee() != null) {
                EmployeeDto dto = new EmployeeDto();
                dto.setEmpId(dgm.getEmployee().getEmp_id());
                dto.setName(dgm.getEmployee().getFirst_name() + " " + dgm.getEmployee().getLast_name());
                dgmEmployees.add(dto);
            }
        }
        return dgmEmployees;
    }

    @Transactional(readOnly = true)
    @Cacheable(value = "campusEmployees", key = "#campusId")
    public List<EmployeeDto> getEmployeeNamesByCampusId(int campusId) {
        entityManager.clear();
        List<CampusProView> views = campusProViewRepository.findByCampusId(campusId);

        return views.stream()
                .map(view -> {
                    int empId = view.getCmps_emp_id();
                    Employee employee = employeeRepository.findById(empId).orElse(null);
                    if (employee != null) {
                        EmployeeDto dto = new EmployeeDto();
                        dto.setEmpId(empId);
                        dto.setName(employee.getFirst_name() + " " + employee.getLast_name());
                        return dto;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public AppStatusTrackView getAppStatusByCampusAndNumber(int appNo, String campusName) {
        return appStatusTrackViewRepository.findByNumAndCmps_name(appNo, campusName)
                .orElseThrow(() -> new NoSuchElementException(
                        "No status record found for App No: " + appNo + " and Campus: " + campusName));
    }
}
