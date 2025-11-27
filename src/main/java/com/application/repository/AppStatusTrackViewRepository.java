package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.AppStatusDTO;
import com.application.entity.AppStatusTrackView;

@Repository
public interface AppStatusTrackViewRepository extends JpaRepository<AppStatusTrackView, Integer>{
	
	Optional<AppStatusTrackView> findByNum(Long num);
	@Query("SELECT a FROM AppStatusTrackView a WHERE a.cmps_id = :cmpsId")
    List<AppStatusTrackView> findByCmps_id(@Param("cmpsId") int cmpsId);
	
	@Query("SELECT a FROM AppStatusTrackView a WHERE a.cmps_id = " +
	           "(SELECT e.campus.campusId FROM Employee e WHERE e.emp_id = :empId)")
	    List<AppStatusTrackView> findByEmployeeCampus(@Param("empId") int empId);
	
	@Query("SELECT a FROM AppStatusTrackView a WHERE a.num = :num AND a.cmps_name = :cmpsName")
	Optional<AppStatusTrackView> findByNumAndCmps_name(@Param("num") int num, @Param("cmpsName") String cmpsName);
	
	 @Query("SELECT new com.application.dto.AppStatusDTO(a.num, a.status, a.cmps_name, a.zone_name) " +
	           "FROM AppStatusTrackView a")
	    List<AppStatusDTO> getAllStatusData();
	 
	 @Query("SELECT new com.application.dto.AppStatusDTO( " +
		       "a.num, " +                 // applicationNo  
		       "a.status, " +              // displayStatus  
		       "a.cmps_name, " +           // campus  
		       "a.zone_name ) " +          // zone
		       "FROM AppStatusTrackView a " +
		       "WHERE a.cmps_id IN :campusIds")
		List<AppStatusDTO> findDTOByCampusIds(@Param("campusIds") List<Integer> campusIds);

}
