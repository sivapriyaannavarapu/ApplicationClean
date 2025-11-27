package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "sce_acdc_lang", schema = "sce_student")
public class AcademicLanguage {

	@Id
	private int acdc_lang_id;
	private String acdc_lang;

	@ManyToOne
	@JoinColumn(name = "lang_id")
	private Language language;

}
