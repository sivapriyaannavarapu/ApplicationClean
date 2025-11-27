package com.application.entity;

import java.time.LocalDate;
import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
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
@Table(name = "sce_stud_acdc_detl", schema = "sce_student")
public class StudentAcademicDetails {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int stud_adms_id;
	@Column(name = "stud_adms_no") 
	private Long studAdmsNo;
	private String ht_no;
	private String first_name;
	private String last_name;
	private LocalDate adms_date;
	private int created_by;
	private LocalDate doj;
//	private Integer orientation_id;
//	@Column(name = "orientation_batch_id")
//	private int orientation_batch_id;
	@Column(name = "pre_school_name")
	private String pre_school_name;
	private String admission_referred_by;
	private String score_app_no;
	private int score_marks;
//	private Date orientation_date;
	private int additional_orientation_fee;
	private Date app_sale_date;
	private Date app_conf_date;
	private int is_active;
	private String apaar_no;
	private String annexure_path;
	private int[] lang_id;
	private String photo_path;
	private int pro_receipt_no;

	@ManyToOne
	@JoinColumn(name = "acdc_year_id")
	private AcademicYear academicYear;

	@ManyToOne
	@JoinColumn(name = "gender_id")
	private Gender gender;

	@ManyToOne
	@JoinColumn(name = "adms_type_id")
	private AdmissionType admissionType;

	@ManyToOne
	@JoinColumn(name = "cmps_id")
	private Campus campus;

	@ManyToOne
	@JoinColumn(name = "stud_type_id")
	private StudentType studentType;

	@ManyToOne
	@JoinColumn(name = "study_type_id")
	private StudyType studyType;

	@ManyToOne
	@JoinColumn(name = "section_id")
	private Section section;

	@ManyToOne
	@JoinColumn(name = "quota_id")
	private Quota quota;

	@ManyToOne
	@JoinColumn(name = "status_id")
	private Status status;

	@ManyToOne
	@JoinColumn(name = "class_id")
	private StudentClass studentClass;

	@ManyToOne
	@JoinColumn(name = "pro_id", referencedColumnName = "emp_id")
	private Employee employee;

	@ManyToOne
	@JoinColumn(name = "pre_school_state_id", referencedColumnName = "state_id")
	private State state;

	@ManyToOne
	@JoinColumn(name = "pre_school_district_id", referencedColumnName = "district_id")
	private District district;

	@ManyToOne
	@JoinColumn(name = "school_type_id")
	private CampusSchoolType campusSchoolType;

//	@ManyToOne
//	@JoinColumn(name = "stream_id")
//	private Stream stream;

//	@ManyToOne
//	@JoinColumn(name = "program_id")
//	private ProgramName programName;

//	@ManyToOne
//	@JoinColumn(name = "exam_program_id")
//	private ExamProgram examProgram;

//	@ManyToOne
//	@JoinColumn(name = "cmps_orientation_id")
//	private CmpsOrientation cmpsOrientation;
	
	@ManyToOne
	@JoinColumn(name = "pre_school_type_id",referencedColumnName = "school_type_id")
	private CampusSchoolType preCampusSchoolType;

}
