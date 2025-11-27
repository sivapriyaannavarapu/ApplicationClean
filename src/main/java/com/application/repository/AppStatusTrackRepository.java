package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.AppStatusTrackDTO;
import com.application.dto.MetricsAggregateDTO;
import com.application.entity.AppStatusTrack;

@Repository
public interface AppStatusTrackRepository extends JpaRepository<AppStatusTrack, Integer> {

	@Query("SELECT new com.application.dto.AppStatusTrackDTO(" +

			"SUM(t.totalApp), SUM(t.appSold), SUM(t.appConfirmed), " +

			"SUM(t.appAvailable), SUM(t.appIssued), SUM(t.appDamaged), " +

			"SUM(t.appUnavailable)) " +

			"FROM AppStatusTrack t WHERE t.isActive = 1")

	Optional<AppStatusTrackDTO> findLatestAggregatedStats();

	@Query("SELECT MAX(a.academicYear.acdcYearId) FROM AppStatusTrack a")
	Integer findLatestYearId();

	@Query("""
			    SELECT
			        SUM(a.totalApp),
			        SUM(a.appSold),
			        SUM(a.appConfirmed),
			        SUM(a.appAvailable),
			        SUM(a.appIssued),
			        SUM(a.appDamaged),
			        SUM(a.appUnavailable)
			    FROM AppStatusTrack a
			    WHERE a.isActive = 1
			""")
	List<Object[]> getOverallTotals();

	@Query("""
			    SELECT SUM(a.totalApp)
			    FROM AppStatusTrack a
			    WHERE a.isActive = 1
			      AND a.issuedByType.appIssuedId = 4
			""")
	Long getOverallWithPro();

	@Query("""
			    SELECT
			        SUM(a.totalApp),
			        SUM(a.appSold),
			        SUM(a.appConfirmed),
			        SUM(a.appAvailable),
			        SUM(a.appIssued),
			        SUM(a.appDamaged),
			        SUM(a.appUnavailable)
			    FROM AppStatusTrack a
			    WHERE a.isActive = 1
			      AND a.academicYear.acdcYearId = :yearId
			""")
	List<Object[]> getTotalsByYear(Integer yearId);

	@Query("""
			    SELECT SUM(a.totalApp)
			    FROM AppStatusTrack a
			    WHERE a.isActive = 1
			      AND a.issuedByType.appIssuedId = 4
			      AND a.academicYear.acdcYearId = :yearId
			""")
	Long getWithProByYear(Integer yearId);

	public record MetricsAggregate(long totalApp, long appSold, long appConfirmed, long appAvailable,
			long appUnavailable, long appDamaged, long appIssued) {
	}

	/**
	 * Aggregates all metric counts for a specific Zone and Academic Year.
	 */
	@Query("SELECT NEW com.application.dto.MetricsAggregateDTO(" + // <-- Use new DTO
			"COALESCE(SUM(ast.totalApp), 0), COALESCE(SUM(ast.appSold), 0), COALESCE(SUM(ast.appConfirmed), 0), "
			+ "COALESCE(SUM(ast.appAvailable), 0), COALESCE(SUM(ast.appUnavailable), 0), "
			+ "COALESCE(SUM(ast.appDamaged), 0), COALESCE(SUM(ast.appIssued), 0)) " + "FROM AppStatusTrack ast "
			+ "WHERE ast.zone.id = :zoneId AND ast.academicYear.acdcYearId = :acdcYearId")
	Optional<MetricsAggregateDTO> getMetricsByZoneAndYear( // <-- Use new DTO
			@Param("zoneId") Long zoneId, @Param("acdcYearId") Integer acdcYearId);

	/**
	 * Aggregates all metric counts for a specific DGM (Employee) and Academic Year.
	 */
	@Query("SELECT NEW com.application.dto.MetricsAggregateDTO("
			+ "COALESCE(SUM(ast.totalApp), 0), COALESCE(SUM(ast.appSold), 0), COALESCE(SUM(ast.appConfirmed), 0), "
			+ "COALESCE(SUM(ast.appAvailable), 0), COALESCE(SUM(ast.appUnavailable), 0), "
			+ "COALESCE(SUM(ast.appDamaged), 0), COALESCE(SUM(ast.appIssued), 0)) " + "FROM AppStatusTrack ast "
			+ "WHERE ast.employee.id = :empId AND ast.academicYear.acdcYearId = :acdcYearId")
	Optional<MetricsAggregateDTO> getMetricsByEmployeeAndYear(@Param("empId") Integer empId,
			@Param("acdcYearId") Integer acdcYearId);

	/**
	 * Aggregates all metric counts for a specific Campus and Academic Year.
	 */
	@Query("SELECT NEW com.application.dto.MetricsAggregateDTO(" + // <-- Use new DTO
			"COALESCE(SUM(ast.totalApp), 0), COALESCE(SUM(ast.appSold), 0), COALESCE(SUM(ast.appConfirmed), 0), "
			+ "COALESCE(SUM(ast.appAvailable), 0), COALESCE(SUM(ast.appUnavailable), 0), "
			+ "COALESCE(SUM(ast.appDamaged), 0), COALESCE(SUM(ast.appIssued), 0)) " + "FROM AppStatusTrack ast "
			+ "WHERE ast.campus.id = :campusId AND ast.academicYear.acdcYearId = :acdcYearId")
	Optional<MetricsAggregateDTO> getMetricsByCampusAndYear( // <-- Use new DTO
			@Param("campusId") Long campusId, @Param("acdcYearId") Integer acdcYearId);

	@Query("SELECT DISTINCT ast.academicYear.acdcYearId FROM AppStatusTrack ast WHERE ast.zone.id = :zoneId")
	List<Integer> findDistinctYearIdsByZone(@Param("zoneId") Long zoneId);

	@Query("SELECT DISTINCT ast.academicYear.acdcYearId FROM AppStatusTrack ast WHERE ast.employee.id = :empId")
	List<Integer> findDistinctYearIdsByEmployee(@Param("empId") Integer empId);

	@Query("SELECT DISTINCT ast.academicYear.acdcYearId FROM AppStatusTrack ast WHERE ast.campus.id = :campusId")
	List<Integer> findDistinctYearIdsByCampus(@Param("campusId") Long campusId);

	// --- NEW: Method for a LIST of campuses (DGM-Rollup) ---
	@Query("SELECT NEW com.application.dto.MetricsAggregateDTO(COALESCE(SUM(ast.totalApp), 0), COALESCE(SUM(ast.appSold), 0), COALESCE(SUM(ast.appConfirmed), 0), COALESCE(SUM(ast.appAvailable), 0), COALESCE(SUM(ast.appUnavailable), 0), COALESCE(SUM(ast.appDamaged), 0), COALESCE(SUM(ast.appIssued), 0)) FROM AppStatusTrack ast WHERE ast.campus.id IN :campusIds AND ast.academicYear.acdcYearId = :acdcYearId")
	Optional<MetricsAggregateDTO> getMetricsByCampusListAndYear(@Param("campusIds") List<Integer> campusIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- NEW: Method for a LIST of campuses (DGM-Rollup) ---
	@Query("SELECT DISTINCT ast.academicYear.acdcYearId FROM AppStatusTrack ast WHERE ast.campus.id IN :campusIds")
	List<Integer> findDistinctYearIdsByCampusList(@Param("campusIds") List<Integer> campusIds);

	// --- NEW: Employee List query for Zonal Rollup (Metrics) ---
	@Query("SELECT NEW com.application.dto.MetricsAggregateDTO(COALESCE(SUM(ast.totalApp), 0), COALESCE(SUM(ast.appSold), 0), COALESCE(SUM(ast.appConfirmed), 0), COALESCE(SUM(ast.appAvailable), 0), COALESCE(SUM(ast.appUnavailable), 0), COALESCE(SUM(ast.appDamaged), 0), COALESCE(SUM(ast.appIssued), 0)) FROM AppStatusTrack ast WHERE ast.employee.id IN :empIds AND ast.academicYear.acdcYearId = :acdcYearId")
	Optional<MetricsAggregateDTO> getMetricsByEmployeeListAndYear(@Param("empIds") List<Integer> empIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- NEW: Employee List query for Zonal Rollup (Years) ---
	@Query("SELECT DISTINCT ast.academicYear.acdcYearId FROM AppStatusTrack ast WHERE ast.employee.id IN :empIds")
	List<Integer> findDistinctYearIdsByEmployeeList(@Param("empIds") List<Integer> empIds);

}