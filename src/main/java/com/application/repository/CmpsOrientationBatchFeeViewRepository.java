package com.application.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.BatchDTO;
import com.application.dto.ClassDTO;
import com.application.dto.GenericDropdownDTO;
import com.application.dto.OrientationBatchDetailsDTO;
import com.application.dto.OrientationDTO;
import com.application.dto.OrientationDropdownDTO;
import com.application.dto.OrientationFeeAndDatesDTO;
import com.application.entity.CmpsOrientationBatchFeeView;

@Repository
public interface CmpsOrientationBatchFeeViewRepository extends JpaRepository<CmpsOrientationBatchFeeView, Integer> {

	List<CmpsOrientationBatchFeeView> findByOrientationId(int orientationId);

	List<CmpsOrientationBatchFeeView> findByCmpsId(int cmpsId);

	@Query("SELECT new com.application.dto.OrientationDTO(c.orientationId, c.orientationName) "
			+ "FROM CmpsOrientationBatchFeeView c WHERE c.classId = :classId")
	List<OrientationDTO> findOrientationsByClassId(int classId);

	@Query("SELECT DISTINCT new com.application.dto.OrientationDTO(c.orientationId, c.orientationName) "
			+ "FROM CmpsOrientationBatchFeeView c " + "WHERE c.classId = :classId " + "AND c.cmpsId = :cmpsId ") // **Condition
																													// for
																													// active
																													// orientation
																													// (is_active
																													// =
																													// 1)**
	List<OrientationDTO> findDistinctOrientationsByClassIdAndCmpsId(int classId, int cmpsId);

	@Query("SELECT DISTINCT new com.application.dto.ClassDTO(c.classId, c.className) "
			+ "FROM CmpsOrientationBatchFeeView c WHERE c.cmpsId = :campusId")
	List<ClassDTO> findClassesByCampusId(int campusId);

	@Query("SELECT DISTINCT new com.application.dto.GenericDropdownDTO(c.studyTypeId, c.studyTypeName) "
			+ "FROM CmpsOrientationBatchFeeView c " + "WHERE c.cmpsId = :cmpsId AND c.classId = :classId")
	List<GenericDropdownDTO> findDistinctStudyTypesByCmpsIdAndClassId(@Param("cmpsId") int cmpsId,
			@Param("classId") int classId);

	@Query("SELECT DISTINCT new com.application.dto.GenericDropdownDTO(c.orientationId, c.orientationName) "
			+ "FROM CmpsOrientationBatchFeeView c "
			+ "WHERE c.cmpsId = :cmpsId AND c.classId = :classId AND c.studyTypeId = :studyTypeId")
	List<GenericDropdownDTO> findDistinctOrientationsByCmpsIdAndClassIdAndStudyTypeId(@Param("cmpsId") int cmpsId,
			@Param("classId") int classId, @Param("studyTypeId") int studyTypeId);

	@Query("SELECT DISTINCT new com.application.dto.BatchDTO(c.orientationBatchId, c.orientationBatchName) "
			+ "FROM CmpsOrientationBatchFeeView c WHERE c.orientationId = :orientationId")
	List<BatchDTO> findDistinctBatchesByOrientationId(@Param("orientationId") int orientationId);

	@Query("SELECT new com.application.dto.OrientationBatchDetailsDTO(c.orientationStartDate, c.orientationEndDate, c.orientationFee) "
			+ "FROM CmpsOrientationBatchFeeView c "
			+ "WHERE c.orientationId = :orientationId AND c.orientationBatchId = :orientationBatchId")
	List<OrientationBatchDetailsDTO> findDetailsByOrientationAndBatchId(@Param("orientationId") int orientationId,
			@Param("orientationBatchId") int orientationBatchId);

	@Query("SELECT DISTINCT new com.application.dto.OrientationDropdownDTO(c.orientationId, c.orientationName) "
			+ "FROM CmpsOrientationBatchFeeView c " + "WHERE c.cmpsId = :campusId AND c.classId = :classId")
	List<OrientationDropdownDTO> findDistinctOrientationsByCampusAndClass(@Param("campusId") int campusId,
			@Param("classId") int classId);

	@Query(value = "SELECT c FROM CmpsOrientationBatchFeeView c "
			+ "WHERE c.orientationId = :orientationId AND c.cmpsId = :cmpsId AND c.classId = :classId "
			+ "ORDER BY c.orientationStartDate ASC " + "LIMIT 1")
	Optional<CmpsOrientationBatchFeeView> findSingleBestBatchDetails(@Param("orientationId") Integer orientationId,
			@Param("cmpsId") Integer cmpsId, @Param("classId") Integer classId);

	@Query("SELECT DISTINCT c.cmpsType FROM CmpsOrientationBatchFeeView c "
			+ "WHERE c.orientationId = :orientationId AND c.cmpsId = :campusId")
	List<String> findDistinctCmpsType(@Param("orientationId") Integer orientationId,
			@Param("campusId") Integer campusId);

	@Query("SELECT DISTINCT new com.application.dto.OrientationFeeAndDatesDTO("
			+ "c.orientationStartDate, c.orientationEndDate, c.orientationFee) " + "FROM CmpsOrientationBatchFeeView c "
			+ "WHERE c.orientationId = :orientationId")
	OrientationFeeAndDatesDTO getOrientationFeeAndDatesDistinct(@Param("orientationId") Integer orientationId);
}
