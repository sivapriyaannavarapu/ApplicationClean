package com.application.service;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.application.dto.DistributionGetTableDTO;
import com.application.entity.Campaign;
import com.application.entity.Dgm;
import com.application.entity.Distribution;
import com.application.repository.CampaignRepository;
import com.application.repository.DgmRepository;
import com.application.repository.DistributionRepository;
 
@Service
public class DistributionGetTableService {
 
    @Autowired
    private DistributionRepository distributionRepository;
 
    @Autowired
    private DgmRepository dgmRepository;
 
    @Autowired
    private CampaignRepository campaignRepository;
    
//    @Cacheable(value = "distributionsByEmployee", key = "#empId")
    public List<DistributionGetTableDTO> getDistributionsByEmployeeAndIssuedToType(int empId, int issuedToTypeId) {

        List<Distribution> distributions = 
                distributionRepository.findByCreatedByAndIssuedToType(empId, issuedToTypeId);

 
        return distributions.stream().map(d -> {
            DistributionGetTableDTO dto = new DistributionGetTableDTO();
 
            // Populate all fields from the Distribution entity
            dto.setAppDistributionId(d.getAppDistributionId());
            dto.setAppStartNo(d.getAppStartNo());
            dto.setAppEndNo(d.getAppEndNo());
            dto.setTotalAppCount(d.getTotalAppCount());
            dto.setAmount(d.getAmount());
            dto.setIsActive(d.getIsActive());
            dto.setCreated_by(d.getCreated_by());
            dto.setIssued_to_emp_id(d.getIssued_to_emp_id());
            dto.setIssueDate(d.getIssueDate());
 
            // Handle related IDs from relationships
            if (d.getIssuedByType() != null) dto.setIssued_by_type_id(d.getIssuedByType().getAppIssuedId());
            if (d.getIssuedToType() != null) dto.setIssued_to_type_id(d.getIssuedToType().getAppIssuedId());
            if (d.getCity() != null) dto.setCity_id(d.getCity().getCityId());
            if (d.getState() != null) dto.setState_id(d.getState().getStateId());
            if (d.getZone() != null) dto.setZone_id(d.getZone().getZoneId());
            if (d.getDistrict() != null) dto.setDistrict_id(d.getDistrict().getDistrictId());
            if (d.getCampus() != null) dto.setCmps_id(d.getCampus().getCampusId());
            if (d.getAcademicYear() != null) dto.setAcdc_year_id(d.getAcademicYear().getAcdcYearId());
            
            // Derive the additional fields
            String issuedToName = null;
            if (d.getIssuedToEmployee() != null) {
                issuedToName = d.getIssuedToEmployee().getFirst_name() + " " + d.getIssuedToEmployee().getLast_name();
            } else if (d.getCampus() != null) {
                issuedToName = d.getCampus().getCampusName();
            } else if (d.getZone() != null) {
                issuedToName = d.getZone().getZoneName();
            } else if (d.getDistrict() != null) {
                issuedToName = d.getDistrict().getDistrictName();
            } else if (d.getCity() != null) {
                issuedToName = d.getCity().getCityName();
            } else if (d.getState() != null) {
                issuedToName = d.getState().getStateName();
            }
            dto.setIssuedToName(issuedToName);
 
            String zoneName = null;
            if (d.getZone() != null) {
                zoneName = d.getZone().getZoneName();
            }
            dto.setZoneName(zoneName);
 
            String campusName = null;
            if (d.getCampus() != null) {
                campusName = d.getCampus().getCampusName();
            }
            dto.setCampusName(campusName);
 
            String dgmName = null;
            // Find DGM name based on zone or campus ID from the distribution record
            if (d.getZone() != null) {
                List<Dgm> dgms = dgmRepository.findByZoneId(d.getZone().getZoneId());
                if (!dgms.isEmpty() && dgms.get(0).getEmployee() != null) {
                    Dgm dgm = dgms.get(0);
                    dgmName = dgm.getEmployee().getFirst_name() + " " + dgm.getEmployee().getLast_name();
                }
            } else if (d.getCampus() != null) {
                List<Dgm> dgms = dgmRepository.findByCampusId(d.getCampus().getCampusId());
                if (!dgms.isEmpty() && dgms.get(0).getEmployee() != null) {
                    Dgm dgm = dgms.get(0);
                    dgmName = dgm.getEmployee().getFirst_name() + " " + dgm.getEmployee().getLast_name();
                }
            }
            dto.setDgmName(dgmName);
            
         // Add logic for campaign area name here
            String campaignAreaName = null;
            if (d.getCampus() != null) {
                Campaign campaign = campaignRepository.findByCampus_CampusId(d.getCampus().getCampusId());
                if (campaign != null) {
                    campaignAreaName = campaign.getAreaName();
                }
            }
            dto.setCampaignAreaName(campaignAreaName);
            
         // Logic for campaign area ID here
            int campaignAreaId = 0; // Initialize with a default value
            if (d.getCampus() != null) {
                Campaign campaign = campaignRepository.findByCampus_CampusId(d.getCampus().getCampusId());
                if (campaign != null) {
                    campaignAreaId = campaign.getCampaignId();
                }
            }
            dto.setCampaignAreaId(campaignAreaId);
            
            return dto;
        }).toList();
    }
}
 

 
 