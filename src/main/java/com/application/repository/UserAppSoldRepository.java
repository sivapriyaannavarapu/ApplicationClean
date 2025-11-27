package com.application.repository;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.GraphSoldSummaryDTO;
import com.application.entity.UserAppSold;

@Repository
public interface UserAppSoldRepository extends JpaRepository<UserAppSold, Long> {

	List<UserAppSold> findByEntityId(Integer entityId);

	@Query("SELECT u.zone.zoneName, SUM(u.totalAppCount), SUM(u.sold) " + "FROM UserAppSold u "
			+ "WHERE u.isActive = 1 AND u.entityId = 2 " + "GROUP BY u.zone.zoneName")
	List<Object[]> getZoneWiseRates();

	@Query("SELECT u.campus.campusName, SUM(u.totalAppCount), SUM(u.sold) " + "FROM UserAppSold u "
			+ "WHERE u.isActive = 1 AND u.entityId = 4 " + "GROUP BY u.campus.campusName")
	List<Object[]> getCampusWiseRates();

	@Query("SELECT CONCAT(e.first_name, ' ', e.last_name), SUM(u.totalAppCount), SUM(u.sold) "
			+ "FROM UserAppSold u JOIN Employee e ON u.empId = e.emp_id " + "WHERE u.isActive = 1 AND u.entityId = 3 "
			+ "GROUP BY e.first_name, e.last_name")
	List<Object[]> getDgmWiseRates();

	@Query("""
			    SELECT
			        a.acdcYearId,
			        SUM(a.totalAppCount),
			        SUM(a.sold)
			    FROM UserAppSold a
			    WHERE a.isActive = 1
			    GROUP BY a.acdcYearId
			    ORDER BY a.acdcYearId
			""")
	List<Object[]> getYearWiseIssuedAndSold();

	@Query("""
			    SELECT NEW com.application.dto.GraphSoldSummaryDTO(
			        COALESCE(SUM(uas.totalAppCount), 0),
			        COALESCE(SUM(uas.sold), 0))
			    FROM UserAppSold uas
			    WHERE uas.empId = :dgmId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByDgm(@Param("dgmId") Integer dgmId,
			@Param("acdcYearId") Integer acdcYearId);

	@Query("""
			    SELECT NEW com.application.dto.GraphSoldSummaryDTO(
			        COALESCE(SUM(uas.totalAppCount), 0),
			        COALESCE(SUM(uas.sold), 0))
			    FROM UserAppSold uas
			    WHERE uas.zone.zoneId = :zoneId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByZone(@Param("zoneId") Integer zoneId,
			@Param("acdcYearId") Integer acdcYearId);

	@Query("""
			    SELECT NEW com.application.dto.GraphSoldSummaryDTO(
			        COALESCE(SUM(uas.totalAppCount), 0),
			        COALESCE(SUM(uas.sold), 0))
			    FROM UserAppSold uas
			    WHERE uas.campus.campusId = :campusId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByCampus(@Param("campusId") Integer campusId,
			@Param("acdcYearId") Integer acdcYearId);

	@Query("""
			    SELECT COALESCE(SUM(uas.totalAppCount), 0)
			    FROM UserAppSold uas
			    WHERE uas.entityId = 4
			      AND uas.zone.zoneId = :zoneId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<Long> getProMetricByZone(@Param("zoneId") Integer zoneId, @Param("acdcYearId") Integer acdcYearId);

	@Query("""
			    SELECT COALESCE(SUM(uas.totalAppCount), 0)
			    FROM UserAppSold uas
			    WHERE uas.campus.campusId = :campusId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<Long> getProMetricByCampus(@Param("campusId") Integer campusId, @Param("acdcYearId") Integer acdcYearId);

	@Query("""
			    SELECT COALESCE(SUM(uas.totalAppCount), 0)
			    FROM UserAppSold uas
			    WHERE uas.empId = :dgmId
			      AND uas.acdcYearId = :acdcYearId
			""")
	Optional<Long> getProMetricByDgm(@Param("dgmId") Integer dgmId, @Param("acdcYearId") Integer acdcYearId);

	// --- NEW: Method for a LIST of campuses (DGM-Rollup) ---
	@Query("SELECT NEW com.application.dto.GraphSoldSummaryDTO(COALESCE(SUM(uas.totalAppCount), 0), COALESCE(SUM(uas.sold), 0)) FROM UserAppSold uas WHERE uas.entityId = 4 AND uas.campus.id IN :campusIds AND uas.acdcYearId = :acdcYearId")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByCampusList(@Param("campusIds") List<Integer> campusIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- NEW: Method for a LIST of campuses (DGM-Rollup) ---
	@Query("SELECT COALESCE(SUM(uas.totalAppCount), 0) FROM UserAppSold uas WHERE uas.entityId = 4 AND uas.campus.id IN :campusIds AND uas.acdcYearId = :acdcYearId")
	Optional<Long> getProMetricByCampusList(@Param("campusIds") List<Integer> campusIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- Methods to find distinct years for GRAPH ---

	@Query("SELECT DISTINCT uas.acdcYearId FROM UserAppSold uas WHERE uas.entityId = 2 AND uas.zone.id = :zoneId")
	List<Integer> findDistinctYearIdsByZone(@Param("zoneId") Integer zoneId);

	@Query("SELECT DISTINCT uas.acdcYearId FROM UserAppSold uas WHERE uas.entityId = 3 AND uas.empId = :dgmId")
	List<Integer> findDistinctYearIdsByDgm(@Param("dgmId") Integer dgmId);

	// This method is for a single campus (PRO role)
	@Query("SELECT DISTINCT uas.acdcYearId FROM UserAppSold uas WHERE uas.entityId = 4 AND uas.campus.id = :campusId")
	List<Integer> findDistinctYearIdsByCampus(@Param("campusId") Integer campusId);

	// --- NEW: Method for a LIST of campuses (DGM-Rollup) ---
	@Query("SELECT DISTINCT uas.acdcYearId FROM UserAppSold uas WHERE uas.entityId = 4 AND uas.campus.id IN :campusIds")
	List<Integer> findDistinctYearIdsByCampusList(@Param("campusIds") List<Integer> campusIds);

	// --- NEW: DGM List query for Zonal Rollup (Graph) ---
	@Query("SELECT NEW com.application.dto.GraphSoldSummaryDTO(COALESCE(SUM(uas.totalAppCount), 0), COALESCE(SUM(uas.sold), 0)) FROM UserAppSold uas WHERE uas.entityId = 3 AND uas.empId IN :dgmEmpIds AND uas.acdcYearId = :acdcYearId")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByDgmList(@Param("dgmEmpIds") List<Integer> dgmEmpIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- NEW: DGM List query for Zonal Rollup ('With PRO' card) ---
	@Query("SELECT COALESCE(SUM(uas.totalAppCount), 0) FROM UserAppSold uas WHERE uas.entityId = 4 AND uas.empId IN :dgmEmpIds AND uas.acdcYearId = :acdcYearId")
	Optional<Long> getProMetricByDgmList(@Param("dgmEmpIds") List<Integer> dgmEmpIds,
			@Param("acdcYearId") Integer acdcYearId);

	// --- NEW: DGM List query for Zonal Rollup (Years) ---
	@Query("SELECT DISTINCT uas.acdcYearId FROM UserAppSold uas WHERE uas.entityId = 3 AND uas.empId IN :dgmEmpIds")
	List<Integer> findDistinctYearIdsByDgmList(@Param("dgmEmpIds") List<Integer> dgmEmpIds);

	@Query("SELECT new com.application.dto.GraphSoldSummaryDTO(SUM(u.totalAppCount), SUM(u.sold)) "
			+ "FROM UserAppSold u " + "WHERE u.zone.zoneId = :zoneId AND u.acdcYearId = :yearId AND u.amount = :amount")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByZoneAndAmount(@Param("zoneId") Integer zoneId,
			@Param("yearId") Integer yearId, @Param("amount") Float amount);

	/**
	 * Custom query to find all distinct academic year IDs that have data for a
	 * specific zone and amount.
	 */
	@Query("SELECT DISTINCT u.acdcYearId FROM UserAppSold u WHERE u.zone.zoneId = :zoneId AND u.amount = :amount")
	List<Integer> findDistinctYearIdsByZoneAndAmount(@Param("zoneId") Integer zoneId, @Param("amount") Float amount);

	@Query("SELECT new com.application.dto.GraphSoldSummaryDTO(SUM(u.totalAppCount), SUM(u.sold)) "
			+ "FROM UserAppSold u " + "WHERE u.campus.id = :campusId AND u.acdcYearId = :yearId AND u.amount = :amount")
	Optional<GraphSoldSummaryDTO> getSalesSummaryByCampusAndAmount(@Param("campusId") Integer campusId,
			@Param("yearId") Integer yearId, @Param("amount") Float amount);

	/**
	 * Custom query to find all distinct academic year IDs that have data for a
	 * specific campus and amount. FIX: Explicitly reference the Campus ID field
	 * (u.campus.id) to prevent type mismatch.
	 */
	@Query("SELECT DISTINCT u.acdcYearId FROM UserAppSold u WHERE u.campus.id = :campusId AND u.amount = :amount")
	List<Integer> findDistinctYearIdsByCampusAndAmount(@Param("campusId") Integer campusId,
			@Param("amount") Float amount);

	@Query("""
			    SELECT
			        a.acdcYearId,
			        COALESCE(SUM(a.totalAppCount), 0),
			        COALESCE(SUM(a.sold), 0)
			    FROM UserAppSold a
			    WHERE a.isActive = 1
			      AND a.empId = :empId
			      AND a.acdcYearId IN :yearIds
			    GROUP BY a.acdcYearId
			    ORDER BY a.acdcYearId
			""")
	List<Object[]> getYearWiseIssuedAndSoldByEmployee(@Param("empId") Integer empId,
			@Param("yearIds") List<Integer> yearIds);

}