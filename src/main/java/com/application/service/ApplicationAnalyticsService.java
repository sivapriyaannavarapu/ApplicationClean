package com.application.service;
 
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.application.dto.CombinedAnalyticsDTO;
import com.application.dto.GraphDTO;
import com.application.dto.GraphSoldSummaryDTO;
import com.application.dto.MetricDTO;
import com.application.dto.MetricsAggregateDTO;
import com.application.dto.MetricsDataDTO;
import com.application.dto.YearlyGraphPointDTO;
import com.application.entity.AcademicYear;
import com.application.entity.SCEmployeeEntity;
import com.application.repository.AcademicYearRepository;
import com.application.repository.AppStatusTrackRepository;
import com.application.repository.DgmRepository;
import com.application.repository.SCEmployeeRepository;
import com.application.repository.UserAppSoldRepository;
 
@Service
public class ApplicationAnalyticsService {
 
    @Autowired
    private UserAppSoldRepository userAppSoldRepository;
 
    @Autowired
    private AppStatusTrackRepository appStatusTrackRepository;
 
    @Autowired
    private AcademicYearRepository academicYearRepository;
    
    @Autowired
    private SCEmployeeRepository scEmployeeRepository;
    
    @Autowired
    private DgmRepository dgmRepository;
 
    // --- NEW "MASTER ROLLUP" ROUTER METHOD ---
    
    /**
     * NEW: This single method handles both DGM and Zonal rollup views.
     * @param empId The employee ID of the DGM or Zonal Accountant.
     * @return The combined analytics for their rollup view.
     */
    public CombinedAnalyticsDTO getRollupAnalytics(Integer empId) {
        
    List<SCEmployeeEntity> employeeList = scEmployeeRepository.findByEmpId(empId);
    
    // Employee not found
    if (employeeList.isEmpty()) {
        // Pass "N/A" for designation since we don't have the employee record
        return createEmptyAnalytics("Invalid Employee", empId, "Employee not found", "N/A"); 
    }
    
    SCEmployeeEntity employee = employeeList.get(0);
    String role = employee.getEmpStudApplicationRole();
    String designation = employee.getDesignationName(); // <--- GET DESIGNATION HERE

    // Null role
    if (role == null) {
         System.err.println("Employee " + empId + " has a null role.");
         return createEmptyAnalytics("Null Role", empId, "Employee has no role", designation); // <--- PASS DESIGNATION
    }

    String trimmedRole = role.trim();

    // --- Router logic ---
    if (trimmedRole.equalsIgnoreCase("DGM")) {
        // If DGM, run the DGM-to-Campus rollup
        return getDgmCampusRollupAnalytics(employee);
        
    } else if (trimmedRole.equalsIgnoreCase("ZONAL ACCOUNTANT")) {
        // If Zonal Accountant, run the Zonal-to-DGM rollup
        return getZonalDgmRollupAnalytics(employee);
        
    } else {
        // If any other role, they don't get this view
        return createEmptyAnalytics(role, empId, "This role does not have a rollup view", designation); // <--- PASS DESIGNATION
    }
}
 
    
    // --- "NORMAL" ROUTER METHOD (Unchanged) ---
    
    /**
     * This is the original "normal" view for DGM, Zonal, or PRO.
     * It shows data for *only* their direct entity.
     */
   public CombinedAnalyticsDTO getAnalyticsForEmployee(Integer empId) {
        
    List<SCEmployeeEntity> employeeList = scEmployeeRepository.findByEmpId(empId);
    
    // Employee not found
    if (employeeList.isEmpty()) {
        System.err.println("No employee found with ID: " + empId);
        // Pass "N/A" for designation
        return createEmptyAnalytics("Invalid Employee", empId, "Employee not found", "N/A"); 
    }
    
    SCEmployeeEntity employee = employeeList.get(0);
    String role = employee.getEmpStudApplicationRole();
    String designation = employee.getDesignationName(); // <--- GET DESIGNATION HERE
    
    // Null role
    if (role == null) {
         System.err.println("Employee " + empId + " has a null role.");
         return createEmptyAnalytics("Null Role", empId, "Employee has no role", designation); // <--- PASS DESIGNATION
    }
    
    String trimmedRole = role.trim();
    CombinedAnalyticsDTO analytics;
    
    if (trimmedRole.equalsIgnoreCase("DGM")) {
        analytics = getDgmAnalytics(empId);
        analytics.setRole("DGM");
        analytics.setEntityName(employee.getFirstName() + " " + employee.getLastName());
        analytics.setEntityId(empId);
        
    } else if (trimmedRole.equalsIgnoreCase("ZONAL ACCOUNTANT")) {
        int zoneId = employee.getZoneId();
        analytics = getZoneAnalytics((long) zoneId);
        analytics.setRole("Zonal Account");
        analytics.setEntityName(employee.getZoneName());
        analytics.setEntityId(zoneId);
        
    } else if (trimmedRole.equalsIgnoreCase("PRO")) {
        int campusId = employee.getEmpCampusId();
        analytics = getCampusAnalytics((long) campusId);
        analytics.setRole("PRO");
        analytics.setEntityName(employee.getCampusName());
        analytics.setEntityId(campusId);
        
    } else {
        System.err.println("Unrecognized role '" + role + "' for empId: " + empId);
        return createEmptyAnalytics(role, empId, "Unrecognized role", designation); // <--- PASS DESIGNATION
    }
    
    // <--- SET DESIGNATION BEFORE RETURNING --->
    analytics.setDesignationName(designation); 
    return analytics;
}
 
    private CombinedAnalyticsDTO createEmptyAnalytics(String role, Integer id, String name, String designationName) {
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();
        analytics.setRole(role);
        analytics.setDesignationName(designationName); // <--- Set designation here
        analytics.setEntityId(id);
        analytics.setEntityName(name);
        return analytics;
    }
    // --- CORE ANALYTICS METHODS (Unchanged) ---
 
    public CombinedAnalyticsDTO getZoneAnalytics(Long zoneId) {
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();
        analytics.setGraphData(getGraphData(
            (yearId) -> userAppSoldRepository.getSalesSummaryByZone(zoneId.intValue(), yearId),
            () -> userAppSoldRepository.findDistinctYearIdsByZone(zoneId.intValue())
        ));
        analytics.setMetricsData(
            getMetricsData(
                (yearId) -> appStatusTrackRepository.getMetricsByZoneAndYear(zoneId, yearId),
                (yearId) -> userAppSoldRepository.getProMetricByZone(zoneId.intValue(), yearId),
                () -> appStatusTrackRepository.findDistinctYearIdsByZone(zoneId)
            )
        );
        return analytics;
    }
 
    public CombinedAnalyticsDTO getDgmAnalytics(Integer dgmEmpId) {
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();
        analytics.setGraphData(getGraphData(
            (yearId) -> userAppSoldRepository.getSalesSummaryByDgm(dgmEmpId, yearId),
            () -> userAppSoldRepository.findDistinctYearIdsByDgm(dgmEmpId)
        ));
        analytics.setMetricsData(
            getMetricsData(
                (yearId) -> appStatusTrackRepository.getMetricsByEmployeeAndYear(dgmEmpId, yearId),
                (yearId) -> userAppSoldRepository.getProMetricByDgm(dgmEmpId, yearId),
                () -> appStatusTrackRepository.findDistinctYearIdsByEmployee(dgmEmpId)
            )
        );
        return analytics;
    }
 
    public CombinedAnalyticsDTO getCampusAnalytics(Long campusId) {
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();
        analytics.setGraphData(getGraphData(
            (yearId) -> userAppSoldRepository.getSalesSummaryByCampus(campusId.intValue(), yearId),
            () -> userAppSoldRepository.findDistinctYearIdsByCampus(campusId.intValue())
        ));
        analytics.setMetricsData(
            getMetricsData(
                (yearId) -> appStatusTrackRepository.getMetricsByCampusAndYear(campusId, yearId),
                (yearId) -> userAppSoldRepository.getProMetricByCampus(campusId.intValue(), yearId),
                () -> appStatusTrackRepository.findDistinctYearIdsByCampus(campusId)
            )
        );
        return analytics;
    }
    
public GraphDTO getGraphDataByZoneIdAndAmount(Integer zoneId, Float amount) {
        
        if (zoneId == null || amount == null) {
            GraphDTO emptyGraph = new GraphDTO();
            emptyGraph.setTitle("Error: Zone ID and Amount must be provided.");
            emptyGraph.setYearlyData(new ArrayList<>());
            return emptyGraph;
        }
        
        // This leverages the generic getGraphData helper with new repository functions
        return getGraphData(
            // Data Fetcher: Function<Integer, Optional<GraphSoldSummaryDTO>> (takes yearId)
            (yearId) -> userAppSoldRepository.getSalesSummaryByZoneAndAmount(zoneId, yearId, amount),
            
            // Year Fetcher: Supplier<List<Integer>> (takes no arguments)
            () -> userAppSoldRepository.findDistinctYearIdsByZoneAndAmount(zoneId, amount)
        );
    }

public GraphDTO getGraphDataByCampusIdAndAmount(Integer campusId, Float amount) {
    
    if (campusId == null || amount == null) {
        GraphDTO emptyGraph = new GraphDTO();
        emptyGraph.setTitle("Error: Campus ID and Amount must be provided.");
        emptyGraph.setYearlyData(new ArrayList<>());
        return emptyGraph;
    }

    // This leverages the generic getGraphData helper with new repository functions
    return getGraphData(
        // Data Fetcher: Function<Integer, Optional<GraphSoldSummaryDTO>> (takes yearId)
        (yearId) -> userAppSoldRepository.getSalesSummaryByCampusAndAmount(campusId, yearId, amount),
        
        // Year Fetcher: Supplier<List<Integer>> (takes no arguments)
        () -> userAppSoldRepository.findDistinctYearIdsByCampusAndAmount(campusId, amount)
    );
}
 
    // --- ROLLUP LOGIC (Now private helpers) ---
    
    /**
     * PRIVATE: Gets analytics for a DGM's *assigned campuses*.
     */
    /**
     * PRIVATE: Gets analytics for a DGM's *assigned campuses*.
     */
    private CombinedAnalyticsDTO getDgmCampusRollupAnalytics(SCEmployeeEntity employee) {
        
        int dgmEmpId = employee.getEmpId();
        String designation = employee.getDesignationName(); // <--- GET DESIGNATION
        List<Integer> campusIds = dgmRepository.findCampusIdsByEmployeeId(dgmEmpId);
        
        if (campusIds.isEmpty()) {
            // Pass designation name in createEmptyAnalytics
            return createEmptyAnalytics(employee.getEmpStudApplicationRole(), dgmEmpId, "DGM manages 0 campuses", designation);
        }

        System.out.println("DGM " + dgmEmpId + " is viewing analytics for " + campusIds.size() + " campuses.");
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();

        analytics.setGraphData(getGraphDataForCampusList(campusIds));
        analytics.setMetricsData(getMetricsDataForCampusList(campusIds));
        
        analytics.setRole("DGM (Campus Rollup)");
        analytics.setDesignationName(designation); // <--- SET DESIGNATION HERE
        analytics.setEntityName(employee.getFirstName() + " " + employee.getLastName());
        analytics.setEntityId(dgmEmpId);
        
        return analytics;
    }

    /**
     * PRIVATE: Gets analytics for a Zonal Accountant's *managed DGMs*.
     */
    private CombinedAnalyticsDTO getZonalDgmRollupAnalytics(SCEmployeeEntity employee) {
        
        int zoneId = employee.getZoneId();
        int empId = employee.getEmpId();
        String designation = employee.getDesignationName(); // <--- GET DESIGNATION
        
        if (zoneId <= 0) {
            // Pass designation name in createEmptyAnalytics
            return createEmptyAnalytics(employee.getEmpStudApplicationRole(), empId, "Zonal Accountant has no valid zone ID", designation);
        }

        List<Integer> dgmEmpIds = dgmRepository.findEmployeeIdsByZoneId(zoneId);
        if (dgmEmpIds.isEmpty()) {
            // Pass designation name in createEmptyAnalytics
            return createEmptyAnalytics(employee.getEmpStudApplicationRole(), empId, "Zonal Accountant manages 0 DGMs", designation);
        }

        System.out.println("Zonal Acct " + empId + " is viewing analytics for " + dgmEmpIds.size() + " DGMs.");
        CombinedAnalyticsDTO analytics = new CombinedAnalyticsDTO();

        analytics.setGraphData(getGraphDataForDgmList(dgmEmpIds));
        analytics.setMetricsData(getMetricsDataForDgmList(dgmEmpIds));
        
        analytics.setRole("Zonal Accountant (DGM Rollup)");
        analytics.setDesignationName(designation); // <--- SET DESIGNATION HERE
        analytics.setEntityName(employee.getZoneName());
        analytics.setEntityId(zoneId);
        
        return analytics;
    }
    
    // --- PRIVATE HELPER METHODS for ROLLUPS (Unchanged) ---
    
    private GraphDTO getGraphDataForCampusList(List<Integer> campusIds) {
        return getGraphData(
            (yearId) -> userAppSoldRepository.getSalesSummaryByCampusList(campusIds, yearId),
            () -> userAppSoldRepository.findDistinctYearIdsByCampusList(campusIds)
        );
    }
    
    private MetricsDataDTO getMetricsDataForCampusList(List<Integer> campusIds) {
        return getMetricsData(
            (yearId) -> appStatusTrackRepository.getMetricsByCampusListAndYear(campusIds, yearId),
            (yearId) -> userAppSoldRepository.getProMetricByCampusList(campusIds, yearId),
            () -> appStatusTrackRepository.findDistinctYearIdsByCampusList(campusIds)
        );
    }
 
    private GraphDTO getGraphDataForDgmList(List<Integer> dgmEmpIds) {
        return getGraphData(
            (yearId) -> userAppSoldRepository.getSalesSummaryByDgmList(dgmEmpIds, yearId),
            () -> userAppSoldRepository.findDistinctYearIdsByDgmList(dgmEmpIds)
        );
    }
    
    private MetricsDataDTO getMetricsDataForDgmList(List<Integer> dgmEmpIds) {
        return getMetricsData(
            (yearId) -> appStatusTrackRepository.getMetricsByEmployeeListAndYear(dgmEmpIds, yearId),
            (yearId) -> userAppSoldRepository.getProMetricByDgmList(dgmEmpIds, yearId),
            () -> appStatusTrackRepository.findDistinctYearIdsByEmployeeList(dgmEmpIds)
        );
    }
 
 
    // --- Private Graph Data Helper (Unchanged) ---
 
    private GraphDTO getGraphData(
            Function<Integer, Optional<GraphSoldSummaryDTO>> dataFetcher,
            Supplier<List<Integer>> yearFetcher) {
        
        GraphDTO graphData = new GraphDTO();
        List<YearlyGraphPointDTO> yearlyDataList = new ArrayList<>();
 
        try {
            List<Integer> existingYearIds = yearFetcher.get();
 
            List<AcademicYear> academicYears = academicYearRepository.findByAcdcYearIdIn(existingYearIds)
                    .stream()
                    .sorted(Comparator.comparingInt(AcademicYear::getAcdcYearId))
                    .toList();
 
            for (AcademicYear year : academicYears) {
                int acdcYearId = year.getAcdcYearId();
                String yearLabel = year.getAcademicYear();
 
                GraphSoldSummaryDTO summary = dataFetcher.apply(acdcYearId)
                        .orElse(new GraphSoldSummaryDTO(0L, 0L));
 
                long issued = summary.totalApplications();
                long sold = summary.totalSold();
 
                double issuedPercent = issued > 0 ? 100.0 : 0.0;
                double soldPercent = (issued > 0)
                        ? Math.min(100.0, ((double) sold / issued) * 100.0)
                        : 0.0;
 
                yearlyDataList.add(new YearlyGraphPointDTO(
                        yearLabel, issuedPercent, soldPercent, issued, sold
                ));
            }
 
            if (!academicYears.isEmpty()) {
                graphData.setTitle("Application Sales Percentage (" +
                        academicYears.get(0).getAcademicYear() + "–" +
                        academicYears.get(academicYears.size() - 1).getAcademicYear() + ")");
            } else {
                graphData.setTitle("Application Sales Percentage (No Data)");
            }
 
        } catch (Exception e) {
            System.err.println("Error fetching graph data: " + e.getMessage());
            e.printStackTrace();
        }
 
        graphData.setYearlyData(yearlyDataList);
        return graphData;
    }
 
    // --- Private Metrics Data Helper (Unchanged) ---
 
    private MetricsDataDTO getMetricsData(
            Function<Integer, Optional<MetricsAggregateDTO>> dataFetcher,
            Function<Integer, Optional<Long>> proFetcher,
            Supplier<List<Integer>> yearFetcher) {

        MetricsDataDTO dto = new MetricsDataDTO();

        try {

            List<Integer> yearIds = yearFetcher.get();

            if (yearIds.isEmpty()) {
                dto.setMetrics(new ArrayList<>());
                return dto;
            }

            // Sort yearIds ascending → last one is current year
            yearIds.sort(Integer::compare);

            int currentYearId = yearIds.get(yearIds.size() - 1);
            int previousYearId = (yearIds.size() > 1)
                    ? yearIds.get(yearIds.size() - 2)
                    : currentYearId;

            AcademicYear cy = academicYearRepository.findById(currentYearId).orElse(null);
            AcademicYear py = academicYearRepository.findById(previousYearId).orElse(null);

            dto.setCurrentYear(cy != null ? cy.getYear() : 0);
            dto.setPreviousYear(py != null ? py.getYear() : 0);

            MetricsAggregateDTO curr = dataFetcher.apply(currentYearId)
                    .orElse(new MetricsAggregateDTO());
            MetricsAggregateDTO prev = dataFetcher.apply(previousYearId)
                    .orElse(new MetricsAggregateDTO());

            long proCurr = proFetcher.apply(currentYearId).orElse(0L);
            long proPrev = proFetcher.apply(previousYearId).orElse(0L);

            MetricsAggregateDTO totalMetrics = curr;   // instead of summing every year
            long totalPro = proCurr;
            // ------------------------------------------------------

            List<MetricDTO> cards = buildMetricsList(curr, prev, totalMetrics, proCurr, proPrev, totalPro);

            dto.setMetrics(cards);

        } catch (Exception ex) {
            System.out.println("🔥 METRICS ERROR: " + ex.getMessage());
            ex.printStackTrace();
            dto.setMetrics(new ArrayList<>());
        }

        return dto;
    }

    /**
     * Builds the metrics list.
     */
    private List<MetricDTO> buildMetricsList(
            MetricsAggregateDTO current, MetricsAggregateDTO previous, MetricsAggregateDTO total,
            long proCurrent, long proPrevious, long totalPro) {
        
        List<MetricDTO> metrics = new ArrayList<>();
 
        metrics.add(createMetric("Total Applications",
            total.totalApp(),
            current.totalApp(), previous.totalApp()));
 
        double soldPercentCurrent = calculatePercentage(current.appSold(), current.totalApp());
        double soldPercentPrevious = calculatePercentage(previous.appSold(), previous.totalApp());
        metrics.add(createMetricWithPercentage("Sold",
            total.appSold(),
            soldPercentCurrent, soldPercentPrevious));
 
        double confirmedPercentCurrent = calculatePercentage(current.appConfirmed(), current.totalApp());
        double confirmedPercentPrevious = calculatePercentage(previous.appConfirmed(), previous.totalApp());
        metrics.add(createMetricWithPercentage("Confirmed",
            total.appConfirmed(),
            confirmedPercentCurrent, confirmedPercentPrevious));
        
        metrics.add(createMetric("Available",
            total.appAvailable(),
            current.appAvailable(), previous.appAvailable()));
 
        long validIssuedCurrent = Math.max(0, current.appIssued());
        long validIssuedPrevious = Math.max(0, previous.appIssued());
        double issuedPercentCurrent = calculatePercentage(validIssuedCurrent, current.totalApp());
        double issuedPercentPrevious = calculatePercentage(validIssuedPrevious, previous.totalApp());
        metrics.add(createMetricWithPercentage("Issued",
            total.appIssued(),
            issuedPercentCurrent, issuedPercentPrevious));
 
        metrics.add(createMetric("Damaged",
            total.appDamaged(),
            current.appDamaged(), previous.appDamaged()));
            
        metrics.add(createMetric("Unavailable",
            total.appUnavailable(),
            current.appUnavailable(), previous.appUnavailable()));
        
        metrics.add(createMetric("With PRO",
            totalPro,
            proCurrent, proPrevious));
 
        return metrics;
    }
 
    // --- UTILITY METHODS ---
 
    private MetricDTO createMetric(String title, long totalValue, long currentValue, long previousValue) {
        double change = calculatePercentageChange(currentValue, previousValue);
        return new MetricDTO(title, totalValue, change, getChangeDirection(change));
    }
 
    private MetricDTO createMetricWithPercentage(String title, long totalValue, double currentPercent, double previousPercent) {
        double change = calculatePercentageChange(currentPercent, previousPercent);
        return new MetricDTO(title, totalValue, change, getChangeDirection(change));
    }
 
    private double calculatePercentage(long numerator, long denominator) {
        if (denominator == 0) return 0.0;
        return (double) Math.max(0, numerator) * 100.0 / denominator;
    }
 
    private double calculatePercentageChange(double current, double previous) {
        if (previous == 0) return (current > 0) ? 100 : 0;
        double change = ((current - previous) / previous) * 100;
        return Math.round(change);
    }
    
    private String getChangeDirection(double change) {
        if (change > 0) return "up";
        if (change < 0) return "down";
        return "neutral";
    }
 
    private int getAcdcYearId(int year) {
        return academicYearRepository.findByYear(year)
                .map(AcademicYear::getAcdcYearId)
                .orElse(0);
    }
}