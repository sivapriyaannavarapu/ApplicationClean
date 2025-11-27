package com.application.service;
 
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.application.dto.AppStatusDTO;
import com.application.entity.AppStatusTrackView;
import com.application.entity.Campus;
import com.application.repository.AppStatusTrackViewRepository;
import com.application.repository.CampusRepository;
 
@Service
public class ApplicationStatusViewService {
 
    @Autowired
    private AppStatusTrackViewRepository appStatusTrackViewRepository;
    @Autowired private CampusRepository campusRepository;

//    @Cacheable(value = "appStatusByCampus", key = "#cmpsId")
    public List<AppStatusTrackView> getApplicationStatusByCampus(int cmpsId) {
        return appStatusTrackViewRepository.findByCmps_id(cmpsId);
    }
    
    public List<AppStatusTrackView> getApplicationStatusByEmployeeCampus(int empId) {
        try {
            List<AppStatusTrackView> result = appStatusTrackViewRepository.findByEmployeeCampus(empId);
            return result;
        } catch (Exception e) {
            throw e;
        }
    }
    
    @Cacheable(value = "allstatustable")
    public List<AppStatusDTO> getAllStatus() {
        return appStatusTrackViewRepository.getAllStatusData();
    }
    
    public List<AppStatusDTO> fetchApplicationStatus(String category, Integer zoneId) {
    	 
        List<Campus> campuses;
 
        // -----------------------------
        // CATEGORY = SCHOOL (business_id = 2)
        // -----------------------------
        if (category.equalsIgnoreCase("school")) {
            int businessId = 2;
 
            if (zoneId != null) {
                campuses = campusRepository.findSchoolCampusesByZone(businessId, zoneId);
            } else {
                campuses = campusRepository.findSchoolCampuses(businessId);
            }
        }
 
        // -----------------------------
        // CATEGORY = COLLEGE (business_id = 1)
        // zone filter ignored
        // -----------------------------
        else if (category.equalsIgnoreCase("college")) {
            int businessId = 1;
            campuses = campusRepository.findCollegeCampuses(businessId);
        }
 
        // -----------------------------
        // INVALID CATEGORY
        // -----------------------------
        else {
            return List.of();
        }
 
        // Extract campus IDs
        List<Integer> campusIds = campuses.stream()
                .map(Campus::getCampusId)
                .toList();
 
        if (campusIds.isEmpty()) {
            return List.of();
        }
 
        // DIRECTLY return DTO from repository (no conversion)
        return appStatusTrackViewRepository.findDTOByCampusIds(campusIds);
    }
 
}