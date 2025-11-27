package com.application.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.application.dto.ConcessionTypeDTO;
import com.application.entity.ConcessionType;

@Repository
public interface ConcessionTypeRepository extends JpaRepository<ConcessionType, Integer> {

	@Query("SELECT new com.application.dto.ConcessionTypeDTO(c.concTypeId, c.conc_type) " + "FROM ConcessionType c "
			+ "WHERE c.conc_type IN :types")
	List<ConcessionTypeDTO> findConcessionTypesByNames(@Param("types") List<String> types);
}
