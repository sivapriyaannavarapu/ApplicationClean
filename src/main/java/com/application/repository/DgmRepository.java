
package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.GenericDropdownDTO;
import com.application.entity.Dgm;

@Repository
public interface DgmRepository extends JpaRepository<Dgm, Integer> {

	@Query("SELECT d FROM Dgm d WHERE d.dgm_id = :dgmId")
	List<Dgm> findByDgmId(@Param("dgmId") int dgmId);

	@Query("SELECT d FROM Dgm d WHERE d.zone.zoneId = :zoneId")
	List<Dgm> findByZoneId(@Param("zoneId") int zoneId);

	@Query("SELECT d FROM Dgm d WHERE d.campus.campusId = :campusId")
	List<Dgm> findByCampusId(@Param("campusId") int campusId);

	@Query("SELECT new com.application.dto.GenericDropdownDTO(e.emp_id, CONCAT(e.first_name, ' ', e.last_name)) "
			+ "FROM Dgm d JOIN d.employee e " + "WHERE e.isActive = 1")
	List<GenericDropdownDTO> findAllDgmEmployees();

	@Query("SELECT DISTINCT new com.application.dto.GenericDropdownDTO(e.emp_id, CONCAT(e.first_name, ' ', e.last_name)) "
			+ "FROM Dgm d " + "JOIN d.employee e " + "WHERE d.zone.zoneId = :zoneId " + "AND e.isActive = 1")
	List<GenericDropdownDTO> findDistinctActiveEmployeesByZoneId(@Param("zoneId") int zoneId);

	@Query("SELECT DISTINCT new com.application.dto.GenericDropdownDTO(c.campusId, c.campusName) " + "FROM Dgm d "
			+ "JOIN d.campus c " + "WHERE c.isActive = 1")
	List<GenericDropdownDTO> findDistinctActiveCampusesByDgm();

	@Query("SELECT d FROM Dgm d WHERE d.employee.emp_id = :empId")
	List<Dgm> findByEmpId(@Param("empId") Integer empId);

	@Query("SELECT DISTINCT d.campus.campusId FROM Dgm d WHERE d.zone.zoneId IN :zoneIds")
	List<Integer> findCampusIdsByZoneIds(List<Integer> zoneIds);

	@Query("SELECT d.campus.id FROM Dgm d WHERE d.employee.id = :empId")
	List<Integer> findCampusIdsByEmployeeId(@Param("empId") Integer empId);

	@Query("SELECT d.employee.id FROM Dgm d WHERE d.zone.id = :zoneId AND d.isActive = 1")
	List<Integer> findEmployeeIdsByZoneId(@Param("zoneId") Integer zoneId);

	@Query("SELECT d FROM Dgm d WHERE d.employee.emp_id = :empId AND d.isActive = :isActive")
	Optional<Dgm> findActiveDgm(@Param("empId") int empId, @Param("isActive") int isActive);

	@Query("SELECT d.zone.zoneId FROM Dgm d WHERE d.employee.emp_id = :empId")
	Optional<Integer> findZoneIdByEmpId(@Param("empId") int empId);

}
