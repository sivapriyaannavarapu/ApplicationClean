package com.application.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.application.dto.AddressDetailsDTO;
import com.application.dto.ApplicationFastDetailsGet;
import com.application.dto.ConcessionConfirmationDTO;
import com.application.dto.ParentSummaryDTO;
import com.application.dto.PaymentDetailsDTO;
import com.application.dto.SiblingDTO;
import com.application.dto.StudentApplicationSaleColegeDTO;
import com.application.dto.StudentApplicationSingleDTO;
import com.application.dto.StudentApplicationUpdateDTO;
import com.application.dto.StudentCollegeConfirmationDto;
import com.application.dto.StudentFastSaleDTO;
import com.application.entity.AcademicYear;
import com.application.entity.Campus;
import com.application.entity.Distribution;
import com.application.entity.Employee;
import com.application.entity.ParentDetails;
import com.application.entity.PaymentDetails;
import com.application.entity.Sibling;
import com.application.entity.Status;
import com.application.entity.StudentAcademicDetails;
import com.application.entity.StudentAddress;
import com.application.entity.StudentApplicationTransaction;
import com.application.entity.StudentClass;
import com.application.entity.StudentConcessionType;
import com.application.entity.StudentOrientationDetails;
import com.application.entity.StudentPersonalDetails;
import com.application.entity.StudentRelation;
import com.application.entity.StudyType;
import com.application.repository.*;
import jakarta.persistence.EntityNotFoundException;
import jakarta.transaction.Transactional;

@Service
public class ApplicationFastSale {

	private final ReligionRepository religionRepository;

	@Autowired
	private DistributionRepository distributionRepository;
	@Autowired
	private AcademicYearRepository academicYearRepository;
	@Autowired
	private QuotaRepository quotaRepository;
	@Autowired
	private GenderRepository genderRepository;
	@Autowired
	private AdmissionTypeRepository admissionTypeRepository;
	@Autowired
	private StudentTypeRepository studentTypeRepository;
	@Autowired
	private StudentClassRepository classRepository;
	@Autowired
	private CampusRepository campusRepository;
	@Autowired
	private StatusRepository statusRepository;
	@Autowired
	private StudentAcademicDetailsRepository studentAcademicDetailsRepository;
	@Autowired
	private StudentPersonalDetailsRepository personalDetailsRepository;
	@Autowired
	private StudentOrientationDetailsRepository orientationDetailsRepository;
	@Autowired
	private StudentRelationRepository relationRepository;
	@Autowired
	private ParentDetailsRepository parentDetailsRepository;
	@Autowired
	private StudyTypeRepository studyTypeRepository;
	@Autowired
	private OrientationRepository orientationRepository;
	@Autowired
	private EmployeeRepository employeeRepository;
	@Autowired
	private SiblingRepository siblingRepository;
	@Autowired
	private ConcessionTypeRepository concessionTypeRepository;
	@Autowired
	private ConcessionReasonRepository concessionReasonRepository;
	@Autowired
	private StudentConcessionTypeRepository concessionRepository;
	@Autowired
	private StateRepository stateRepository;
	@Autowired
	private CityRepository cityRepository;
	@Autowired
	private DistrictRepository districtRepository;
	@Autowired
	private MandalRepository mandalRepository;
	@Autowired
	private StudentAddressRepository studentAddressRepository;
	@Autowired
	private PaymentModeRepository paymentModeRepository;
	@Autowired
	private PaymentDetailsRepository paymentDetailsRepository;
	@Autowired
	private OrgBankBranchRepository orgBankBranchRepository;
	@Autowired
	private OrgBankRepository orgBankRepository;
	@Autowired
	private StudentApplicationTransactionRepository studentApplicationTransactionRepository;
	@Autowired
	private SectorRepository sectorRepository;
	@Autowired
	private OccupationRepository occupationRepository;
	@Autowired
	private FoodTypeRepository foodTypeRepository;
	@Autowired
	private BloodGroupRepository bloodGroupRepository;
	@Autowired
	private CasteRepository casteRepository;
	@Autowired
	private CmpsOrientationBatchFeeViewRepository cmpsOrientationBatchFeeViewRepository;
	@Autowired
	CampusSchoolTypeRepository schoolTypeRepository;

	ApplicationFastSale(EmployeeRepository employeeRepository, ReligionRepository religionRepository,
			DgmRepository dgmRepository) {
		this.employeeRepository = employeeRepository;
		this.religionRepository = religionRepository;
	}

	// Application Fast SALE - college
	@Transactional
	public StudentAcademicDetails createFastSaleAdmission(StudentFastSaleDTO formData) {

		Long admissionNumberNumeric = formData.getStudAdmsNo();
		if (admissionNumberNumeric == null) {
			throw new IllegalArgumentException("Admission Number must be provided.");
		}
		Distribution distribution = distributionRepository.findProDistributionForAdmissionNumber(admissionNumberNumeric)
				.orElseThrow(() -> new EntityNotFoundException(
						"No PRO has been assigned for Admission Number: " + admissionNumberNumeric));
		Employee pro = distribution.getIssuedToEmployee();
		if (pro == null) {
			throw new EntityNotFoundException(
					"A PRO has not been linked to the distribution for Admission Number: " + admissionNumberNumeric);
		}

		// --- 1. Save Academic Details (only image fields + essentials) ---
		StudentAcademicDetails academicDetails = new StudentAcademicDetails();
		academicDetails.setIs_active(1); // Active

		// Set from DTO
		if (formData.getAcademicYearId() != null) {
			academicYearRepository.findById(formData.getAcademicYearId()).ifPresent(academicDetails::setAcademicYear);
		}
		academicDetails.setStudAdmsNo(formData.getStudAdmsNo());
		academicDetails.setFirst_name(formData.getFirstName());
		academicDetails.setLast_name(formData.getLastName());
		academicDetails.setAdms_date(LocalDate.now());
		academicDetails.setApaar_no(formData.getApaarNo());
		academicDetails.setApp_sale_date(new Date());

		academicDetails.setAdmission_referred_by(formData.getAdmissionReferredBy());

		// Quota (Admission referred by)
		if (formData.getQuotaId() != null) {
			quotaRepository.findById(formData.getQuotaId()).ifPresent(academicDetails::setQuota);
		}

		// Gender
		if (formData.getGenderId() != null) {
			genderRepository.findById(formData.getGenderId()).ifPresent(academicDetails::setGender);
		}

		if (formData.getAppTypeId() != null)
			admissionTypeRepository.findById(formData.getAppTypeId()).ifPresent(academicDetails::setAdmissionType);

		// Student Type
		if (formData.getStudentTypeId() != null) {
			studentTypeRepository.findById(formData.getStudentTypeId()).ifPresent(academicDetails::setStudentType);
		}

		// Class and Campus/Branch
		StudentClass studentClass = classRepository.findById(formData.getClassId())
				.orElseThrow(() -> new EntityNotFoundException("Invalid Class ID: " + formData.getClassId()));
		academicDetails.setStudentClass(studentClass);

		Campus campus = campusRepository.findById(formData.getBranchId())
				.orElseThrow(() -> new EntityNotFoundException("Invalid Branch ID: " + formData.getBranchId()));
		academicDetails.setCampus(campus);

		// Employee/PRO (from lookup)
		academicDetails.setCreated_by(formData.getCreatedBy());
		academicDetails.setEmployee(pro);

		// Defaults
		StudyType defaultStudyType = studyTypeRepository.findById(1)
				.orElseThrow(() -> new EntityNotFoundException("Default StudyType (ID: 1) not found"));
		academicDetails.setStudyType(defaultStudyType);

		Status defaultStatus = statusRepository.findById(9)
				.orElseThrow(() -> new EntityNotFoundException("Default Status (ID: 2) not found"));
		academicDetails.setStatus(defaultStatus);

		StudentAcademicDetails savedAcademicDetails = studentAcademicDetailsRepository.save(academicDetails);

		// --- 2. Save Personal Details (minimal: DOB, defaults) ---
		StudentPersonalDetails personalDetails = new StudentPersonalDetails();
		personalDetails.setStudentAcademicDetails(savedAcademicDetails);
		personalDetails.setDob(formData.getDob());
		personalDetails.setCreated_by(formData.getCreatedBy());
		personalDetails.setStud_aadhaar_no(formData.getAadharCardNo());
		personalDetailsRepository.save(personalDetails);

		// --- 3. Save Student Orientation Details ---
		StudentOrientationDetails orientationDetails = new StudentOrientationDetails();
		orientationDetails.setStudentAcademicDetails(savedAcademicDetails);
		if (formData.getOrientationId() != null) {
			orientationRepository.findById(formData.getOrientationId()).ifPresent(orientationDetails::setOrientation);
		}
		orientationDetailsRepository.save(orientationDetails);

		// --- 4. Save Parent Details (Father only, if provided) ---
		if (formData.getFatherName() != null || formData.getFatherMobileNo() != null) {

			StudentRelation fatherRelation = relationRepository.findById(1) // Assuming 1 = Father
					.orElseThrow(() -> new EntityNotFoundException("StudentRelation 'Father' (ID: 1) not found"));

			ParentDetails parent = new ParentDetails();
			parent.setStudentAcademicDetails(savedAcademicDetails);
			parent.setName(formData.getFatherName());
			parent.setMobileNo(formData.getFatherMobileNo());
			parent.setCreated_by(formData.getCreatedBy());
			parent.setStudentRelation(fatherRelation);
			// Defaults
			parent.setOccupation("Not Provided");
			parent.setEmail("not provided");
			parentDetailsRepository.save(parent);
		}

		if (formData.getAddressDetails() != null) {
			AddressDetailsDTO addressDTO = formData.getAddressDetails();
			StudentAddress address = new StudentAddress();
			address.setStudentAcademicDetails(savedAcademicDetails);
			address.setHouse_no(addressDTO.getDoorNo());
			address.setStreet(addressDTO.getStreet());
			address.setLandmark(addressDTO.getLandmark());
			address.setArea(addressDTO.getArea());
			if (addressDTO.getPincode() != null)
				address.setPostalCode(addressDTO.getPincode());
			if (addressDTO.getStateId() != null)
				stateRepository.findById(addressDTO.getStateId()).ifPresent(address::setState);
			if (addressDTO.getCityId() != null)
				cityRepository.findById(addressDTO.getCityId()).ifPresent(address::setCity);
			if (addressDTO.getMandalId() != null)
				mandalRepository.findById(addressDTO.getMandalId()).ifPresent(address::setMandal);
			if (addressDTO.getDistrictId() != null)
				districtRepository.findById(addressDTO.getDistrictId()).ifPresent(address::setDistrict);

			// --- FIX 1: Use the 'createdBy' from the AddressDetailsDTO ---
			address.setCreated_by(addressDTO.getCreatedBy());

			studentAddressRepository.save(address);
		}

		PaymentDetailsDTO paymentDTO = formData.getPaymentDetails();

		if (paymentDTO != null && paymentDTO.getAmount() != null) {

			PaymentDetails paymentDetails = new PaymentDetails();

			paymentDetails.setStudentAcademicDetails(savedAcademicDetails);
			paymentDetails.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
			paymentDetails.setPre_print_receipt_no(paymentDTO.getPrePrintedReceiptNo());
			paymentDetails.setRemarks(paymentDTO.getRemarks());
			paymentDetails.setCreated_by(paymentDTO.getCreatedBy());
			paymentDetails.setApp_fee(paymentDTO.getAmount());
			paymentDetails.setPaid_amount(paymentDTO.getAmount());
			paymentDetails.setAcedemicYear(savedAcademicDetails.getAcademicYear());
			paymentDetails.setStudentClass(savedAcademicDetails.getStudentClass());
			paymentDetails.setStatus(defaultStatus);

			if (paymentDTO.getPaymentModeId() != null) {
				paymentModeRepository.findById(paymentDTO.getPaymentModeId()).ifPresent(paymentDetails::setPaymenMode);
			}

			PaymentDetails savedPaymentDetails = paymentDetailsRepository.save(paymentDetails);

			Integer paymentModeId = paymentDTO.getPaymentModeId();
			final int DD_PAYMENT_ID = 2;
			final int CHEQUE_PAYMENT_ID = 3;

			if (paymentModeId != null && (paymentModeId == DD_PAYMENT_ID || paymentModeId == CHEQUE_PAYMENT_ID)) {

				StudentApplicationTransaction transaction = new StudentApplicationTransaction();
				transaction.setPaymentDetails(savedPaymentDetails);
				transaction.setPaymentMode(savedPaymentDetails.getPaymenMode());
				transaction.setNumber(paymentDTO.getTransactionNumber());
				transaction.setDate(paymentDTO.getTransactionDate());
				transaction.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
				transaction.setCreated_by(paymentDTO.getCreatedBy());
				transaction.setStatus("Pending");

				// DD / Cheque extra fields
				transaction.setIfsc_code(paymentDTO.getIfscCode());
				if (paymentDTO.getOrganisationId() != null)
					transaction.setOrg_id(paymentDTO.getOrganisationId());
				if (paymentDTO.getBankId() != null)
					orgBankRepository.findById(paymentDTO.getBankId()).ifPresent(transaction::setOrgBank);
				if (paymentDTO.getBranchId() != null)
					orgBankBranchRepository.findById(paymentDTO.getBranchId()).ifPresent(transaction::setOrgBankBranch);
				if (paymentDTO.getCityId() != null)
					cityRepository.findById(paymentDTO.getCityId()).ifPresent(transaction::setCity);

				studentApplicationTransactionRepository.save(transaction);
			}
		}

		return savedAcademicDetails;
	}

	// Application FastSale Get
	public ApplicationFastDetailsGet getFastSaleDetailsByAdmissionNo(Long studAdmsNo) {

		StudentAcademicDetails student = studentAcademicDetailsRepository.findByStudAdmsNo(studAdmsNo)
				.orElseThrow(() -> new EntityNotFoundException("Student not found with Admission No: " + studAdmsNo));

		Optional<StudentPersonalDetails> personalOpt = personalDetailsRepository.findByStudentAcademicDetails(student);
		Optional<StudentOrientationDetails> orientationOpt = orientationDetailsRepository
				.findByStudentAcademicDetails(student);
		Optional<ParentDetails> fatherOpt = parentDetailsRepository
				.findByStudentAcademicDetailsAndStudentRelationRelationId(student, 1);
		Optional<StudentAddress> addressOpt = studentAddressRepository.findByStudentAcademicDetails(student);

		ApplicationFastDetailsGet detailsDTO = new ApplicationFastDetailsGet();

		// --- Core Admission Fields ---
		detailsDTO.setFirstName(student.getFirst_name());
		detailsDTO.setLastName(student.getLast_name());
		detailsDTO.setApaarNo(student.getApaar_no());

		// Admission Referred By
		String referredBy = student.getAdmission_referred_by();

		if (referredBy != null && !referredBy.trim().isEmpty()) {

			try {
				// Try treat as employee ID
				Integer empId = Integer.valueOf(referredBy);
				detailsDTO.setAdmissionReferredById(empId);

				employeeRepository.findById(empId).ifPresentOrElse(emp -> {

					// FIXED: use actual column names from your entity
					String fullName = emp.getFirst_name()
							+ (emp.getLast_name() != null ? " " + emp.getLast_name() : "");

					detailsDTO.setAdmissionReferredByName(fullName);

				}, () -> {
					// Employee not found → fallback
					detailsDTO.setAdmissionReferredByName(referredBy);
				});

			} catch (NumberFormatException e) {
				// Not a number → treat as plain name
				detailsDTO.setAdmissionReferredById(null);
				detailsDTO.setAdmissionReferredByName(referredBy);
			}

		} else {
			detailsDTO.setAdmissionReferredById(null);
			detailsDTO.setAdmissionReferredByName(null);
		}

		// Gender
		if (student.getGender() != null) {
			detailsDTO.setGenderId(student.getGender().getGender_id());
			detailsDTO.setGenderName(student.getGender().getGenderName());
		}

		// Quota
		if (student.getQuota() != null) {
			detailsDTO.setQuotaId(student.getQuota().getQuota_id());
			detailsDTO.setQuotaName(student.getQuota().getQuota_name());
		}

		// Admission Type
		if (student.getAdmissionType() != null) {
			detailsDTO.setAdmissionTypeId(student.getAdmissionType().getAdms_type_id());
			detailsDTO.setAdmissionTypeName(student.getAdmissionType().getAdms_type_name());
		}

		// Academic Year
		if (student.getAcademicYear() != null) {
			detailsDTO.setAcademicYearId(student.getAcademicYear().getAcdcYearId());
			detailsDTO.setAcademicYearValue(student.getAcademicYear().getAcademicYear());
		}

		// Branch (Campus)
		if (student.getCampus() != null) {
			Campus campus = student.getCampus();

			detailsDTO.setBranchId(campus.getCampusId());
			detailsDTO.setBranchName(campus.getCampusName());

			// --- NEW: City from Campus ---
			if (campus.getCity() != null) {
				detailsDTO.setCityId(campus.getCity().getCityId());
				detailsDTO.setCityName(campus.getCity().getCityName());
			}
		}

		// Student Type
		if (student.getStudentType() != null) {
			detailsDTO.setStudentTypeId(student.getStudentType().getStud_type_id());
			detailsDTO.setStudentTypeName(student.getStudentType().getStud_type());
		}

		// Class
		if (student.getStudentClass() != null) {
			detailsDTO.setJoiningClassId(student.getStudentClass().getClassId());
			detailsDTO.setJoiningClassName(student.getStudentClass().getClassName());
		}

		// Personal Details
		personalOpt.ifPresent(personal -> {
			detailsDTO.setDob(personal.getDob());
			detailsDTO.setAadharCardNo(personal.getStud_aadhaar_no());
		});

		// Orientation + Fee
		Integer cmpsId = student.getCampus() != null ? student.getCampus().getCampusId() : null;
		Integer classId = student.getStudentClass() != null ? student.getStudentClass().getClassId() : null;

		Integer orientationId = orientationOpt.flatMap(orientation -> Optional.ofNullable(orientation.getOrientation()))
				.map(orientation -> {
					detailsDTO.setOrientationId(orientation.getOrientationId());
					detailsDTO.setOrientationName(orientation.getOrientation_name());
					return orientation.getOrientationId();
				}).orElse(null);

		if (orientationId != null && cmpsId != null && classId != null) {
			cmpsOrientationBatchFeeViewRepository.findSingleBestBatchDetails(orientationId, cmpsId, classId)
					.ifPresent(feeView -> {
						detailsDTO.setOrientationStartDate(feeView.getOrientationStartDate());
						detailsDTO.setOrientationEndDate(feeView.getOrientationEndDate());
						detailsDTO.setOrientationFee(feeView.getOrientationFee());
					});
		}

		// Parent Info
		fatherOpt.ifPresent(
				father -> detailsDTO.setParentInfo(new ParentSummaryDTO(father.getName(), father.getMobileNo())));

		// Address
		addressOpt.ifPresent(address -> {
			AddressDetailsDTO addr = new AddressDetailsDTO();
			addr.setDoorNo(address.getHouse_no());
			addr.setStreet(address.getStreet());
			addr.setLandmark(address.getLandmark());
			addr.setArea(address.getArea());
			addr.setPincode(address.getPostalCode());
			addr.setCreatedBy(address.getCreated_by());

			if (address.getState() != null)
				addr.setStateId(address.getState().getStateId());
			if (address.getCity() != null)
				addr.setCityId(address.getCity().getCityId());
			if (address.getMandal() != null)
				addr.setMandalId(address.getMandal().getMandal_id());
			if (address.getDistrict() != null)
				addr.setDistrictId(address.getDistrict().getDistrictId());

			detailsDTO.setAddressDetails(addr);
		});

		return detailsDTO;
	}

	// ApplicationSale - Colleges
	@Transactional
	public StudentAcademicDetails createApplicationSale(StudentApplicationSaleColegeDTO formData) {

		Long admissionNumberNumeric = formData.getStudAdmsNo();
		if (admissionNumberNumeric == null) {
			throw new IllegalArgumentException("Admission Number must be provided.");
		}

		// ==============================================================
		// PART 1: VALIDATE PRO and LOOKUP/FETCH Academic Record
		// ==============================================================
		Distribution distribution = distributionRepository.findProDistributionForAdmissionNumber(admissionNumberNumeric)
				.orElseThrow(() -> new EntityNotFoundException(
						"No PRO has been assigned for Admission Number: " + admissionNumberNumeric));
		Employee pro = distribution.getIssuedToEmployee();
		if (pro == null) {
			throw new EntityNotFoundException(
					"A PRO has not been linked to the distribution for Admission Number: " + admissionNumberNumeric);
		}

		// --- CRITICAL FIX: Fetch existing Academic Details or create a new one
		// (UPSERT) ---
		StudentAcademicDetails academicDetails = studentAcademicDetailsRepository
				.findByStudAdmsNo(admissionNumberNumeric).orElseGet(StudentAcademicDetails::new);

		// --- 1. Map/Update Academic Details (Conditional Update) ---
		academicDetails.setIs_active(1);
		academicDetails.setStudAdmsNo(formData.getStudAdmsNo());

		// Conditional Updates for Core Identity (To prevent overwriting Fast Sale data
		// with null)
		if (formData.getFirstName() != null)
			academicDetails.setFirst_name(formData.getFirstName());
		if (formData.getLastName() != null)
			academicDetails.setLast_name(formData.getLastName());
		if (formData.getApaarNo() != null)
			academicDetails.setApaar_no(formData.getApaarNo());
		if (formData.getGenderId() != null)
			genderRepository.findById(formData.getGenderId()).ifPresent(academicDetails::setGender);
		if (formData.getQuotaId() != null)
			quotaRepository.findById(formData.getQuotaId()).ifPresent(academicDetails::setQuota);
		if (formData.getAppTypeId() != null)
			admissionTypeRepository.findById(formData.getAppTypeId()).ifPresent(academicDetails::setAdmissionType);
		if (formData.getAcademicYearId() != null)
			academicYearRepository.findById(formData.getAcademicYearId()).ifPresent(academicDetails::setAcademicYear);
		if (formData.getStudentTypeId() != null)
			studentTypeRepository.findById(formData.getStudentTypeId()).ifPresent(academicDetails::setStudentType);
		if (formData.getSchoolType() != null)
			schoolTypeRepository.findById(formData.getSchoolType()).ifPresent(academicDetails::setCampusSchoolType);

		// Fields that must be set/updated regardless of prior state
		academicDetails.setAdms_date(LocalDate.now());
		academicDetails.setApp_sale_date(formData.getAppSaleDate());
		academicDetails.setAdmission_referred_by(formData.getAdmissionReferedBy());

		if (formData.getProReceiptNo() != null) {
			academicDetails.setPro_receipt_no(formData.getProReceiptNo().intValue());
		}

		// New/Detailed Academic Fields
		academicDetails.setHt_no(formData.getHallTicketNumber());
		academicDetails.setScore_app_no(formData.getScoreAppNo());
		if (formData.getScoreMarks() != null) {
			academicDetails.setScore_marks(formData.getScoreMarks());
		}

		// Previous School Details
		academicDetails.setPre_school_name(formData.getSchoolName());
		if (formData.getSchoolStateId() != null && formData.getSchoolStateId() > 0)
			stateRepository.findById(formData.getSchoolStateId()).ifPresent(academicDetails::setState);
		if (formData.getSchoolDistrictId() != null && formData.getSchoolDistrictId() > 0)
			districtRepository.findById(formData.getSchoolDistrictId()).ifPresent(academicDetails::setDistrict);

		if (formData.getClassId() != null) {
			StudentClass studentClass = classRepository.findById(formData.getClassId())
					.orElseThrow(() -> new EntityNotFoundException("Invalid Class ID: " + formData.getClassId()));
			academicDetails.setStudentClass(studentClass);
		}

		if (formData.getBranchId() != null) {
			Campus campus = campusRepository.findById(formData.getBranchId())
					.orElseThrow(() -> new EntityNotFoundException("Invalid Branch ID: " + formData.getBranchId()));
			academicDetails.setCampus(campus);
		}

		academicDetails.setCreated_by(formData.getCreatedBy());
		academicDetails.setEmployee(pro);

		// Defaults/Status Update
		StudyType defaultStudyType = studyTypeRepository.findById(1)
				.orElseThrow(() -> new EntityNotFoundException("Default StudyType (ID: 1) not found"));
		academicDetails.setStudyType(defaultStudyType);

		Status defaultStatus = statusRepository.findById(2)
				.orElseThrow(() -> new EntityNotFoundException("Default Status (ID: 2) not found"));
		academicDetails.setStatus(defaultStatus);

		StudentAcademicDetails savedAcademicDetails = studentAcademicDetailsRepository.save(academicDetails);

		// --- 2. Save/Update Personal Details ---
		StudentPersonalDetails personalDetails = personalDetailsRepository
				.findByStudentAcademicDetails(savedAcademicDetails).orElseGet(StudentPersonalDetails::new);

		personalDetails.setStudentAcademicDetails(savedAcademicDetails);
		personalDetails.setCreated_by(formData.getCreatedBy());

		if (formData.getAadharCardNo() != null)
			personalDetails.setStud_aadhaar_no(formData.getAadharCardNo());
		if (formData.getDob() != null)
			personalDetails.setDob(formData.getDob());

		if (formData.getCasteId() != null)
			casteRepository.findById(formData.getCasteId()).ifPresent(personalDetails::setCaste);
		if (formData.getReligionId() != null)
			religionRepository.findById(formData.getReligionId()).ifPresent(personalDetails::setReligion);
		if (formData.getBloodGroupId() != null)
			bloodGroupRepository.findById(formData.getBloodGroupId()).ifPresent(personalDetails::setBloodGroup);
		if (formData.getFoodTypeId() != null)
			foodTypeRepository.findById(formData.getFoodTypeId()).ifPresent(personalDetails::setFoodType);

		personalDetailsRepository.save(personalDetails);

		// --- 3. Save/Update Student Orientation Details ---
		StudentOrientationDetails orientationDetails = orientationDetailsRepository
				.findByStudentAcademicDetails(savedAcademicDetails).orElseGet(StudentOrientationDetails::new);

		orientationDetails.setStudentAcademicDetails(savedAcademicDetails);
		if (formData.getOrientationId() != null)
			orientationRepository.findById(formData.getOrientationId()).ifPresent(orientationDetails::setOrientation);
		orientationDetailsRepository.save(orientationDetails);

		// --- 4. Save/Update Parent Details (Father and Mother) ---
		StudentRelation fatherRelation = relationRepository.findById(1)
				.orElseThrow(() -> new EntityNotFoundException("StudentRelation 'Father' (ID: 1) not found"));

		// FATHER UPSERT
		if (formData.getFatherName() != null || formData.getFatherMobileNo() != null) {
			ParentDetails father = parentDetailsRepository
					.findByStudentAcademicDetailsAndStudentRelationRelationId(savedAcademicDetails, 1)
					.orElseGet(ParentDetails::new);

			father.setStudentAcademicDetails(savedAcademicDetails);
			father.setStudentRelation(fatherRelation);
			father.setCreated_by(formData.getCreatedBy());

			if (formData.getFatherName() != null)
				father.setName(formData.getFatherName());
			if (formData.getFatherMobileNo() != null)
				father.setMobileNo(formData.getFatherMobileNo());

			father.setEmail(formData.getFatherEmail());
			if (formData.getFatherSectorId() != null)
				sectorRepository.findById(formData.getFatherSectorId()).ifPresent(father::setSector);
			if (formData.getFatherOccupationId() != null)
				occupationRepository.findById(formData.getFatherOccupationId())
						.ifPresent(occupation -> father.setOccupation(occupation.getOccupation_name()));
			parentDetailsRepository.save(father);
		}

		// MOTHER UPSERT
		if (formData.getMotherName() != null || formData.getMotherMobileNo() != null) {
			StudentRelation motherRelation = relationRepository.findById(2)
					.orElseThrow(() -> new EntityNotFoundException("StudentRelation 'Mother' (ID: 2) not found"));

			ParentDetails mother = parentDetailsRepository
					.findByStudentAcademicDetailsAndStudentRelationRelationId(savedAcademicDetails, 2)
					.orElseGet(ParentDetails::new);

			mother.setStudentAcademicDetails(savedAcademicDetails);
			mother.setStudentRelation(motherRelation);
			mother.setCreated_by(formData.getCreatedBy());

			mother.setName(formData.getMotherName());
			mother.setMobileNo(formData.getMotherMobileNo());
			mother.setEmail(formData.getMotherEmail());
			if (formData.getMotherSectorId() != null)
				sectorRepository.findById(formData.getMotherSectorId()).ifPresent(mother::setSector);
			if (formData.getMotherOccupationId() != null)
				occupationRepository.findById(formData.getMotherOccupationId())
						.ifPresent(occupation -> mother.setOccupation(occupation.getOccupation_name()));
			parentDetailsRepository.save(mother);
		}

		// --- 5. Save/Update Address Details ---
		if (formData.getAddressDetails() != null) {
			AddressDetailsDTO addressDTO = formData.getAddressDetails();

			StudentAddress address = studentAddressRepository.findByStudentAcademicDetails(savedAcademicDetails)
					.orElseGet(StudentAddress::new);

			address.setStudentAcademicDetails(savedAcademicDetails);
			address.setHouse_no(addressDTO.getDoorNo());
			address.setStreet(addressDTO.getStreet());
			address.setLandmark(addressDTO.getLandmark());
			address.setArea(addressDTO.getArea());
			if (addressDTO.getPincode() != null)
				address.setPostalCode(addressDTO.getPincode());

			// CRITICAL FIX: Ensure ID is present and non-zero before calling findById
			if (addressDTO.getStateId() != null && addressDTO.getStateId() > 0)
				stateRepository.findById(addressDTO.getStateId()).ifPresent(address::setState);
			if (addressDTO.getCityId() != null && addressDTO.getCityId() > 0)
				cityRepository.findById(addressDTO.getCityId()).ifPresent(address::setCity);
			if (addressDTO.getMandalId() != null && addressDTO.getMandalId() > 0)
				mandalRepository.findById(addressDTO.getMandalId()).ifPresent(address::setMandal);
			if (addressDTO.getDistrictId() != null && addressDTO.getDistrictId() > 0) // <--- FIX HERE
				districtRepository.findById(addressDTO.getDistrictId()).ifPresent(address::setDistrict);

			address.setCreated_by(addressDTO.getCreatedBy());
			studentAddressRepository.save(address);
		}

		// --- 6. Save/Update Siblings (UPSERT LOGIC) ---
		if (formData.getSiblings() != null && !formData.getSiblings().isEmpty()) {
			Map<String, Sibling> existingSiblingsMap = siblingRepository
					.findByStudentAcademicDetails(savedAcademicDetails).stream()
					.filter(s -> s.getSibling_name() != null)
					.collect(Collectors.toMap(Sibling::getSibling_name, Function.identity(), (first, second) -> first));

			for (SiblingDTO siblingDto : formData.getSiblings()) {
				Sibling sibling = existingSiblingsMap.get(siblingDto.getFullName());
				if (sibling == null) {
					sibling = new Sibling();
					sibling.setStudentAcademicDetails(savedAcademicDetails);
					sibling.setCreated_by(siblingDto.getCreatedBy());
					sibling.setSibling_name(siblingDto.getFullName());
				}
				sibling.setSibling_school(siblingDto.getSchoolName());
				if (siblingDto.getRelationTypeId() != null)
					relationRepository.findById(siblingDto.getRelationTypeId()).ifPresent(sibling::setStudentRelation);
				if (siblingDto.getClassId() != null)
					classRepository.findById(siblingDto.getClassId()).ifPresent(sibling::setStudentClass);
				if (siblingDto.getGenderId() != null)
					genderRepository.findById(siblingDto.getGenderId()).ifPresent(sibling::setGender);
				siblingRepository.save(sibling);
			}
		}

		// --- 7. Save/Update Concession Details (UPSERT LOGIC) ---
		if (formData.getConcessions() != null && !formData.getConcessions().isEmpty()) {
			Map<Integer, StudentConcessionType> existingConcessionsMap = concessionRepository
					.findByStudAdmsId(savedAcademicDetails.getStud_adms_id()).stream()
					.filter(c -> c.getConcessionType() != null).collect(Collectors.toMap(
							c -> c.getConcessionType().getConcTypeId(), Function.identity(), (first, second) -> first));

			AcademicYear currentYear = academicYearRepository
					.findById(savedAcademicDetails.getAcademicYear().getAcdcYearId())
					.orElseThrow(() -> new EntityNotFoundException("Academic Year not found"));

			for (ConcessionConfirmationDTO concDto : formData.getConcessions()) {
				StudentConcessionType concession = existingConcessionsMap.get(concDto.getConcessionTypeId());
				if (concession == null) {
					concession = new StudentConcessionType();
					concession.setStudAdmsId(savedAcademicDetails.getStud_adms_id());
					concession.setAcademicYear(currentYear);
					concession.setCreated_by(concDto.getCreatedBy());
					concession.setCreated_Date(LocalDateTime.now());
					if (concDto.getConcessionTypeId() != null)
						concessionTypeRepository.findById(concDto.getConcessionTypeId())
								.ifPresent(concession::setConcessionType);
				}
				concession.setConc_amount(concDto.getConcessionAmount());
				concession.setComments(concDto.getComments());
				if (concDto.getReasonId() != null)
					concessionReasonRepository.findById(concDto.getReasonId())
							.ifPresent(concession::setConcessionReason);
				concession.setConc_referred_by(concDto.getConcReferedBy());
				if (concDto.getGivenById() != null)
					concession.setConc_issued_by(concDto.getGivenById());
				if (concDto.getAuthorizedById() != null)
					concession.setConc_authorised_by(concDto.getAuthorizedById());
				concessionRepository.save(concession);
			}
		}

		// --- 8. Save NEW Payment and Transaction Details ---
		PaymentDetailsDTO paymentDTO = formData.getPaymentDetails();
		if (paymentDTO != null && paymentDTO.getAmount() != null) {

			PaymentDetails paymentDetails = new PaymentDetails();
			paymentDetails.setStudentAcademicDetails(savedAcademicDetails);
			paymentDetails.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
			paymentDetails.setPre_print_receipt_no(paymentDTO.getPrePrintedReceiptNo());
			paymentDetails.setRemarks(paymentDTO.getRemarks());
			paymentDetails.setCreated_by(paymentDTO.getCreatedBy());
			paymentDetails.setApp_fee(paymentDTO.getAmount());
			paymentDetails.setPaid_amount(paymentDTO.getAmount());
			paymentDetails.setAcedemicYear(savedAcademicDetails.getAcademicYear());
			paymentDetails.setStudentClass(savedAcademicDetails.getStudentClass());
			paymentDetails.setStatus(defaultStatus);
			if (paymentDTO.getPaymentModeId() != null)
				paymentModeRepository.findById(paymentDTO.getPaymentModeId()).ifPresent(paymentDetails::setPaymenMode);

			PaymentDetails savedPaymentDetails = paymentDetailsRepository.save(paymentDetails);

			// Transaction (Only for DD/Cheque)
			Integer paymentModeId = paymentDTO.getPaymentModeId();
			final int DD_PAYMENT_ID = 2;
			final int CHEQUE_PAYMENT_ID = 3;

			if (paymentModeId != null && (paymentModeId == DD_PAYMENT_ID || CHEQUE_PAYMENT_ID == paymentModeId)) {
				StudentApplicationTransaction transaction = new StudentApplicationTransaction();
				transaction.setPaymentDetails(savedPaymentDetails);
				transaction.setPaymentMode(savedPaymentDetails.getPaymenMode());
				transaction.setNumber(paymentDTO.getTransactionNumber());
				transaction.setDate(paymentDTO.getTransactionDate());
				transaction.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
				transaction.setCreated_by(paymentDTO.getCreatedBy());
				transaction.setStatus("Pending");

				// Transaction location details
				transaction.setIfsc_code(paymentDTO.getIfscCode());
				if (paymentDTO.getOrganisationId() != null)
					transaction.setOrg_id(paymentDTO.getOrganisationId());
				if (paymentDTO.getBankId() != null)
					orgBankRepository.findById(paymentDTO.getBankId()).ifPresent(transaction::setOrgBank);
				if (paymentDTO.getBranchId() != null)
					orgBankBranchRepository.findById(paymentDTO.getBranchId()).ifPresent(transaction::setOrgBankBranch);
				if (paymentDTO.getCityId() != null)
					cityRepository.findById(paymentDTO.getCityId()).ifPresent(transaction::setCity);

				studentApplicationTransactionRepository.save(transaction);
			}
		}

		return savedAcademicDetails;
	}

	// application-sale - colleges - get

	@Transactional
	public StudentApplicationSingleDTO getSingleApplicationDetails(Long studAdmsNo) {

		// 1. Fetch Academic Entity
		StudentAcademicDetails academic = studentAcademicDetailsRepository.findByStudAdmsNo(studAdmsNo)
				.orElseThrow(() -> new EntityNotFoundException("Student not found"));

		StudentApplicationSingleDTO dto = new StudentApplicationSingleDTO();

		// --- Academic Basic ---
		dto.setStudAdmsId(academic.getStud_adms_id());
		dto.setStudAdmsNo(academic.getStudAdmsNo());
		dto.setFirstName(academic.getFirst_name());
		dto.setLastName(academic.getLast_name());
		dto.setApaarNo(academic.getApaar_no());
		dto.setAppSaleDate(academic.getApp_sale_date());
		dto.setHallTicketNo(academic.getHt_no());
		dto.setScoreMarks(academic.getScore_marks());
		dto.setPreSchoolName(academic.getPre_school_name());

		// ============================================
		// 1️⃣ FIX: Admission Referred By (String → ID/Name)
		// ============================================
		String ref = academic.getAdmission_referred_by();

		if (ref != null && !ref.trim().isEmpty()) {
			try {
				Integer empId = Integer.valueOf(ref);
				dto.setAdmissionReferredByID(empId);

				employeeRepository.findById(empId).ifPresentOrElse(emp -> {
					String fullName = emp.getFirst_name()
							+ (emp.getLast_name() != null ? " " + emp.getLast_name() : "");
					dto.setAdmissionReferredByName(fullName);
				}, () -> dto.setAdmissionReferredByName(ref));

			} catch (NumberFormatException e) {
				dto.setAdmissionReferredByID(null);
				dto.setAdmissionReferredByName(ref); // plain string
			}
		}

		// --- Academic Lookups ---
		if (academic.getAcademicYear() != null) {
			dto.setAcademicYearId(academic.getAcademicYear().getAcdcYearId());
			dto.setAcademicYearName(academic.getAcademicYear().getAcademicYear());
		}
		if (academic.getStudentClass() != null) {
			dto.setClassId(academic.getStudentClass().getClassId());
			dto.setClassName(academic.getStudentClass().getClassName());
		}
		if (academic.getCampus() != null) {

			Campus campus = academic.getCampus();

			dto.setBranchId(campus.getCampusId());
			dto.setBranchName(campus.getCampusName());

			// ============================================
			// 2️⃣ FIX: City from Campus
			// ============================================
			if (campus.getCity() != null) {
				dto.setCityId(campus.getCity().getCityId());
				dto.setCityName(campus.getCity().getCityName());
			}
		}

		if (academic.getQuota() != null) {
			dto.setQuotaId(academic.getQuota().getQuota_id());
			dto.setQuotaName(academic.getQuota().getQuota_name());
		}
		if (academic.getGender() != null) {
			dto.setGenderId(academic.getGender().getGender_id());
			dto.setGenderName(academic.getGender().getGenderName());
		}
		if (academic.getAdmissionType() != null) {
			dto.setAdmissionTypeId(academic.getAdmissionType().getAdms_type_id());
			dto.setAdmissionTypeName(academic.getAdmissionType().getAdms_type_name());
		}
		if (academic.getStudentType() != null) {
			dto.setStudentTypeId(academic.getStudentType().getStud_type_id());
			dto.setStudentTypeName(academic.getStudentType().getStud_type());
		}
		if (academic.getStudyType() != null) {
			dto.setStudyTypeId(academic.getStudyType().getStudy_type_id());
			dto.setStudyTypeName(academic.getStudyType().getStudy_type_name());
		}

		// Pre-School State & District
		if (academic.getState() != null) {
			dto.setPreSchoolStateId(academic.getState().getStateId());
			dto.setPreSchoolStateName(academic.getState().getStateName());
		}
		if (academic.getDistrict() != null) {
			dto.setPreSchoolDistrictId(academic.getDistrict().getDistrictId());
			dto.setPreSchoolDistrictName(academic.getDistrict().getDistrictName());
		}

		// --- Personal Details ---
		personalDetailsRepository.findByStudentAcademicDetails(academic).ifPresent(personal -> {
			dto.setAadharNo(personal.getStud_aadhaar_no());
			dto.setDob(personal.getDob());

			if (personal.getCaste() != null) {
				dto.setCasteId(personal.getCaste().getCaste_id());
				dto.setCasteName(personal.getCaste().getCaste_type());
			}
			if (personal.getReligion() != null) {
				dto.setReligionId(personal.getReligion().getReligion_id());
				dto.setReligionName(personal.getReligion().getReligion_type());
			}
			if (personal.getBloodGroup() != null) {
				dto.setBloodGroupId(personal.getBloodGroup().getBlood_group_id());
				dto.setBloodGroupName(personal.getBloodGroup().getBlood_group_name());
			}
			if (personal.getFoodType() != null) {
				dto.setFoodTypeId(personal.getFoodType().getFood_type_id());
				dto.setFoodTypeName(personal.getFoodType().getFood_type());
			}
		});

		// --- Orientation Details ---
		StudentOrientationDetails orientation = orientationDetailsRepository.findByStudentAcademicDetails(academic)
				.orElse(null);

		if (orientation != null && orientation.getOrientation() != null) {

			dto.setOrientationId(orientation.getOrientation().getOrientationId());
			dto.setOrientationName(orientation.getOrientation().getOrientation_name());

			Integer orientationId = orientation.getOrientation().getOrientationId();
			Integer cmpsId = academic.getCampus() != null ? academic.getCampus().getCampusId() : null;
			Integer classId = academic.getStudentClass() != null ? academic.getStudentClass().getClassId() : null;

			if (orientationId != null && cmpsId != null && classId != null) {
				cmpsOrientationBatchFeeViewRepository.findSingleBestBatchDetails(orientationId, cmpsId, classId)
						.ifPresent(feeView -> {
							dto.setOrientationStartDate(feeView.getOrientationStartDate());
							dto.setOrientationEndDate(feeView.getOrientationEndDate());
							dto.setOrientationFee(feeView.getOrientationFee());
						});
			}
		}

		// --- Parents ---
		List<ParentDetails> parents = parentDetailsRepository.findByStudentAcademicDetails(academic);

		for (ParentDetails p : parents) {

			if (p.getStudentRelation().getRelationId() == 1) { // Father
				dto.setFatherName(p.getName());
				dto.setFatherMobile(p.getMobileNo());
				dto.setFatherEmail(p.getEmail());
				dto.setFatherOccupationName(p.getOccupation());

				if (p.getSector() != null) {
					dto.setFatherSectorId(p.getSector().getOccupation_sector_id());
					dto.setFatherSectorName(p.getSector().getSector_name());
				}

			} else if (p.getStudentRelation().getRelationId() == 2) { // Mother

				dto.setMotherName(p.getName());
				dto.setMotherMobile(p.getMobileNo());
				dto.setMotherEmail(p.getEmail());
				dto.setMotherOccupationName(p.getOccupation());

				if (p.getSector() != null) {
					dto.setMotherSectorId(p.getSector().getOccupation_sector_id());
					dto.setMotherSectorName(p.getSector().getSector_name());
				}
			}
		}

		// --- Address ---
		studentAddressRepository.findByStudentAcademicDetails(academic).ifPresent(address -> {
			dto.setDoorNo(address.getHouse_no());
			dto.setStreet(address.getStreet());
			dto.setArea(address.getArea());
			dto.setLandmark(address.getLandmark());
			dto.setPincode(address.getPostalCode());

			if (address.getState() != null) {
				dto.setAddressStateId(address.getState().getStateId());
				dto.setAddressStateName(address.getState().getStateName());
			}
			if (address.getDistrict() != null) {
				dto.setAddressDistrictId(address.getDistrict().getDistrictId());
				dto.setAddressDistrictName(address.getDistrict().getDistrictName());
			}
			if (address.getCity() != null) {
				dto.setAddressCityId(address.getCity().getCityId());
				dto.setAddressCityName(address.getCity().getCityName());
			}
			if (address.getMandal() != null) {
				dto.setAddressMandalId(address.getMandal().getMandal_id());
				dto.setAddressMandalName(address.getMandal().getMandal_name());
			}
		});

		// --- Siblings ---
		siblingRepository.findByStudentAcademicDetails(academic).forEach(s -> {
			StudentApplicationSingleDTO.SiblingItem item = new StudentApplicationSingleDTO.SiblingItem();

			item.setFullName(s.getSibling_name());
			item.setSchoolName(s.getSibling_school());

			if (s.getStudentClass() != null) {
				item.setClassId(s.getStudentClass().getClassId());
				item.setClassName(s.getStudentClass().getClassName());
			}
			if (s.getStudentRelation() != null) {
				item.setRelationId(s.getStudentRelation().getRelationId());
				item.setRelationName(s.getStudentRelation().getRelationType());
			}

			dto.getSiblings().add(item);
		});

		// --- Concessions ---
		concessionRepository.findByStudAdmsId(academic.getStud_adms_id())
	    .forEach(c -> {

	        StudentApplicationSingleDTO.ConcessionItem item =
	                new StudentApplicationSingleDTO.ConcessionItem();

	        // Amount + Comments
	        item.setAmount(c.getConc_amount());
	        item.setComments(c.getComments());

	        // Type
	        if (c.getConcessionType() != null) {
	            item.setConcessionTypeId(c.getConcessionType().getConcTypeId());
	            item.setConcessionTypeName(c.getConcessionType().getConc_type());
	        }

	        // Reason
	        if (c.getConcessionReason() != null) {
	            item.setReasonId(c.getConcessionReason().getConc_reason_id());
	            item.setReasonName(c.getConcessionReason().getConc_reason());
	        }

	        // Posted By Fields
	     // Given By
	        if (c.getConc_issued_by() > 0) {
	            item.setGivenById(c.getConc_issued_by());
	            employeeRepository.findById(c.getConc_issued_by()).ifPresent(emp -> {
	                item.setGivenByName(emp.getFirst_name() +
	                   (emp.getLast_name() != null ? " " + emp.getLast_name() : ""));
	            });
	        }

	        // Authorized By
	        if (c.getConc_authorised_by() > 0) {
	            item.setAuthorizedById(c.getConc_authorised_by());
	            employeeRepository.findById(c.getConc_authorised_by()).ifPresent(emp -> {
	                item.setAuthorizedByName(emp.getFirst_name() +
	                   (emp.getLast_name() != null ? " " + emp.getLast_name() : ""));
	            });
	        }

	        // Referred By
	        if (c.getConc_referred_by() > 0) {
	            item.setConcReferedBy(c.getConc_referred_by());
	            employeeRepository.findById(c.getConc_referred_by()).ifPresent(emp -> {
	                item.setConcReferedByName(emp.getFirst_name() +
	                   (emp.getLast_name() != null ? " " + emp.getLast_name() : ""));
	            });
	        }


	        dto.getConcessions().add(item);
	    });

		return dto;
	}

	// ApplicationSale -college - upadte
	@Transactional
	public String updateApplicationSale(Long studAdmsNo, StudentApplicationUpdateDTO formData) {

		// ==============================================================
		// 1. FETCH EXISTING ENTITIES (UPSERT)
		// ==============================================================
		StudentAcademicDetails academicDetails = studentAcademicDetailsRepository.findByStudAdmsNo(studAdmsNo)
				.orElseThrow(() -> new EntityNotFoundException("Student not found: " + studAdmsNo));

		StudentPersonalDetails personalDetails = personalDetailsRepository.findByStudentAcademicDetails(academicDetails)
				.orElseGet(StudentPersonalDetails::new);

		StudentOrientationDetails orientationDetails = orientationDetailsRepository
				.findByStudentAcademicDetails(academicDetails).orElseGet(StudentOrientationDetails::new);

		StudentAddress address = studentAddressRepository.findByStudentAcademicDetails(academicDetails)
				.orElseGet(StudentAddress::new);

		// ==============================================================
		// 2. UPDATE ACADEMIC DETAILS (NULL-SAFE & ZERO-SAFE)
		// ==============================================================

		if (formData.getAcademicYearId() != null && formData.getAcademicYearId() > 0) {
			academicYearRepository.findById(formData.getAcademicYearId()).ifPresent(academicDetails::setAcademicYear);
		}

		if (formData.getFirstName() != null) {
			academicDetails.setFirst_name(formData.getFirstName());
		}

		if (formData.getLastName() != null) {
			academicDetails.setLast_name(formData.getLastName());
		}

		if (formData.getApaarNo() != null) {
			academicDetails.setApaar_no(formData.getApaarNo());
		}

		if (formData.getAppSaleDate() != null) {
			academicDetails.setApp_sale_date(formData.getAppSaleDate());
		}

		if (formData.getProReceiptNo() != null) {
			academicDetails.setPro_receipt_no(formData.getProReceiptNo().intValue());
		}

		if (formData.getAdmissionReferredBy() != null) {
			academicDetails.setAdmission_referred_by(formData.getAdmissionReferredBy());
		}

		if (formData.getQuotaId() != null && formData.getQuotaId() > 0) {
			quotaRepository.findById(formData.getQuotaId()).ifPresent(academicDetails::setQuota);
		}

		if (formData.getGenderId() != null && formData.getGenderId() > 0) {
			genderRepository.findById(formData.getGenderId()).ifPresent(academicDetails::setGender);
		}

		if (formData.getAppTypeId() != null && formData.getAppTypeId() > 0) {
			admissionTypeRepository.findById(formData.getAppTypeId()).ifPresent(academicDetails::setAdmissionType);
		}

		if (formData.getStudentTypeId() != null && formData.getStudentTypeId() > 0) {
			studentTypeRepository.findById(formData.getStudentTypeId()).ifPresent(academicDetails::setStudentType);
		}

		if (formData.getHallTicketNumber() != null) {
			academicDetails.setHt_no(formData.getHallTicketNumber());
		}

		if (formData.getScoreAppNo() != null) {
			academicDetails.setScore_app_no(formData.getScoreAppNo());
		}

		if (formData.getScoreMarks() != null) {
			academicDetails.setScore_marks(formData.getScoreMarks());
		}

		if (formData.getSchoolName() != null) {
			academicDetails.setPre_school_name(formData.getSchoolName());
		}

		if (formData.getSchoolStateId() != null && formData.getSchoolStateId() > 0) {
			stateRepository.findById(formData.getSchoolStateId()).ifPresent(academicDetails::setState);
		}

		if (formData.getSchoolDistrictId() != null && formData.getSchoolDistrictId() > 0) {
			districtRepository.findById(formData.getSchoolDistrictId()).ifPresent(academicDetails::setDistrict);
		}

		// Class update only if client sends valid id
		if (formData.getClassId() != null && formData.getClassId() > 0) {
			classRepository.findById(formData.getClassId()).ifPresent(academicDetails::setStudentClass);
		}

		// Branch update only if client sends valid id
		if (formData.getBranchId() != null && formData.getBranchId() > 0) {
			campusRepository.findById(formData.getBranchId()).ifPresent(academicDetails::setCampus);
		}

		StudentAcademicDetails updatedAcademicDetails = studentAcademicDetailsRepository.save(academicDetails);

		// ==============================================================
		// 3. UPDATE PERSONAL DETAILS (NULL-SAFE & ZERO-SAFE)
		// ==============================================================
		personalDetails.setStudentAcademicDetails(updatedAcademicDetails);

		if (formData.getAadharCardNo() != null) {
			personalDetails.setStud_aadhaar_no(formData.getAadharCardNo());
		}

		if (formData.getDob() != null) {
			personalDetails.setDob(formData.getDob());
		}

		if (formData.getCasteId() != null && formData.getCasteId() > 0) {
			casteRepository.findById(formData.getCasteId()).ifPresent(personalDetails::setCaste);
		}

		if (formData.getReligionId() != null && formData.getReligionId() > 0) {
			religionRepository.findById(formData.getReligionId()).ifPresent(personalDetails::setReligion);
		}

		if (formData.getBloodGroupId() != null && formData.getBloodGroupId() > 0) {
			bloodGroupRepository.findById(formData.getBloodGroupId()).ifPresent(personalDetails::setBloodGroup);
		}

		// FOOD TYPE - important: ignore null or zero
		if (formData.getFoodTypeId() != null && formData.getFoodTypeId() > 0) {
			foodTypeRepository.findById(formData.getFoodTypeId()).ifPresent(personalDetails::setFoodType);
		}

		personalDetailsRepository.save(personalDetails);

		// ==============================================================
		// 4. UPDATE ORIENTATION (NULL-SAFE)
		// ==============================================================
		orientationDetails.setStudentAcademicDetails(updatedAcademicDetails);
		if (formData.getOrientationId() != null && formData.getOrientationId() > 0) {
			orientationRepository.findById(formData.getOrientationId()).ifPresent(orientationDetails::setOrientation);
		}
		orientationDetailsRepository.save(orientationDetails);

		// ==============================================================
		// 5. UPDATE PARENT DETAILS (NULL-SAFE UPSERT)
		// ==============================================================
		List<ParentDetails> parents = parentDetailsRepository.findByStudentAcademicDetails(updatedAcademicDetails);

		ParentDetails father = parents.stream()
				.filter(p -> p.getStudentRelation() != null && p.getStudentRelation().getRelationId() == 1).findFirst()
				.orElseGet(() -> {
					ParentDetails p = new ParentDetails();
					p.setStudentAcademicDetails(updatedAcademicDetails);
					p.setStudentRelation(relationRepository.findById(1).orElse(null));
					return p;
				});

		ParentDetails mother = parents.stream()
				.filter(p -> p.getStudentRelation() != null && p.getStudentRelation().getRelationId() == 2).findFirst()
				.orElseGet(() -> {
					ParentDetails p = new ParentDetails();
					p.setStudentAcademicDetails(updatedAcademicDetails);
					p.setStudentRelation(relationRepository.findById(2).orElse(null));
					return p;
				});

		// Father update
		if (formData.getFatherName() != null || formData.getFatherMobileNo() != null
				|| formData.getFatherEmail() != null || formData.getFatherSectorId() != null
				|| formData.getFatherOccupationId() != null) {

			father.setStudentAcademicDetails(updatedAcademicDetails);
			if (father.getStudentRelation() == null) {
				father.setStudentRelation(relationRepository.findById(1)
						.orElseThrow(() -> new EntityNotFoundException("Father relation missing")));
			}

			if (formData.getFatherName() != null)
				father.setName(formData.getFatherName());
			if (formData.getFatherMobileNo() != null)
				father.setMobileNo(formData.getFatherMobileNo());
			if (formData.getFatherEmail() != null)
				father.setEmail(formData.getFatherEmail());

			if (formData.getFatherSectorId() != null && formData.getFatherSectorId() > 0) {
				sectorRepository.findById(formData.getFatherSectorId()).ifPresent(father::setSector);
			}

			if (formData.getFatherOccupationId() != null && formData.getFatherOccupationId() > 0) {
				// You currently store occupation as String in ParentDetails.
				// We fetch occupation entity and store its name into the occupation column.
				occupationRepository.findById(formData.getFatherOccupationId())
						.ifPresent(occupation -> father.setOccupation(occupation.getOccupation_name()));
			}

			// Only set created_by if new (optional)
			if (father.getCreated_by() == 0 && formData.getCreatedBy() != null) {
				father.setCreated_by(formData.getCreatedBy());
			}

			if (formData.getFatherSectorId() != null && formData.getFatherSectorId() > 0) {
				sectorRepository.findById(formData.getFatherSectorId()).ifPresent(father::setSector);
			}

			parentDetailsRepository.save(father);
		}

		// Mother update
		if (formData.getMotherName() != null || formData.getMotherMobileNo() != null
				|| formData.getMotherEmail() != null || formData.getMotherSectorId() != null
				|| formData.getMotherOccupationId() != null) {

			mother.setStudentAcademicDetails(updatedAcademicDetails);
			if (mother.getStudentRelation() == null) {
				mother.setStudentRelation(relationRepository.findById(2)
						.orElseThrow(() -> new EntityNotFoundException("Mother relation missing")));
			}

			if (formData.getMotherName() != null)
				mother.setName(formData.getMotherName());
			if (formData.getMotherMobileNo() != null)
				mother.setMobileNo(formData.getMotherMobileNo());
			if (formData.getMotherEmail() != null)
				mother.setEmail(formData.getMotherEmail());

			if (formData.getMotherSectorId() != null && formData.getMotherSectorId() > 0) {
				sectorRepository.findById(formData.getMotherSectorId()).ifPresent(mother::setSector);
			}

			if (formData.getMotherOccupationId() != null && formData.getMotherOccupationId() > 0) {
				occupationRepository.findById(formData.getMotherOccupationId())
						.ifPresent(occupation -> mother.setOccupation(occupation.getOccupation_name()));
			}

			if (mother.getCreated_by() == 0 && formData.getCreatedBy() != null) {
				mother.setCreated_by(formData.getCreatedBy());
			}

			parentDetailsRepository.save(mother);
		}

		// ==============================================================
		// 6. UPDATE SIBLINGS
		// ==============================================================
		if (formData.getSiblings() != null) {
			Map<String, Sibling> existing = siblingRepository.findByStudentAcademicDetails(updatedAcademicDetails)
					.stream().filter(s -> s.getSibling_name() != null)
					.collect(Collectors.toMap(Sibling::getSibling_name, Function.identity(), (a, b) -> a));

			for (SiblingDTO s : formData.getSiblings()) {
				Sibling sib = existing.getOrDefault(s.getFullName(), new Sibling());
				sib.setStudentAcademicDetails(updatedAcademicDetails);

				if (s.getFullName() != null)
					sib.setSibling_name(s.getFullName());
				if (s.getSchoolName() != null)
					sib.setSibling_school(s.getSchoolName());
				if (s.getRelationTypeId() != null && s.getRelationTypeId() > 0)
					relationRepository.findById(s.getRelationTypeId()).ifPresent(sib::setStudentRelation);
				if (s.getClassId() != null && s.getClassId() > 0)
					classRepository.findById(s.getClassId()).ifPresent(sib::setStudentClass);
				if (s.getGenderId() != null && s.getGenderId() > 0)
					genderRepository.findById(s.getGenderId()).ifPresent(sib::setGender);

				if (sib.getCreated_by() == 0 && s.getCreatedBy() != null) {
					sib.setCreated_by(s.getCreatedBy());
				}

				siblingRepository.save(sib);
			}
		}

		// ==============================================================
		// 7. UPDATE CONCESSIONS
		// ==============================================================
		if (formData.getConcessions() != null) {
			Map<Integer, StudentConcessionType> existingMap = concessionRepository
					.findByStudAdmsId(updatedAcademicDetails.getStud_adms_id()).stream()
					.filter(c -> c.getConcessionType() != null).collect(Collectors
							.toMap(c -> c.getConcessionType().getConcTypeId(), Function.identity(), (a, b) -> a));

			AcademicYear year = updatedAcademicDetails.getAcademicYear();

			for (ConcessionConfirmationDTO c : formData.getConcessions()) {

				StudentConcessionType conc = existingMap.getOrDefault(c.getConcessionTypeId(),
						new StudentConcessionType());
				conc.setStudAdmsId(updatedAcademicDetails.getStud_adms_id());
				conc.setAcademicYear(year);

				if (c.getConcessionTypeId() != null && c.getConcessionTypeId() > 0)
					concessionTypeRepository.findById(c.getConcessionTypeId()).ifPresent(conc::setConcessionType);

				if (c.getConcessionAmount() != null)
					conc.setConc_amount(c.getConcessionAmount());

				conc.setComments(c.getComments());

				if (c.getReasonId() != null && c.getReasonId() > 0)
					concessionReasonRepository.findById(c.getReasonId()).ifPresent(conc::setConcessionReason);

				conc.setConc_referred_by(c.getConcReferedBy());
				conc.setConc_issued_by(c.getGivenById());
				conc.setConc_authorised_by(c.getAuthorizedById());

				concessionRepository.save(conc);
			}
		}

		// ==============================================================
		// 8. UPDATE ADDRESS
		// ==============================================================
		if (formData.getAddressDetails() != null) {
			AddressDetailsDTO ad = formData.getAddressDetails();

			address.setStudentAcademicDetails(updatedAcademicDetails);

			if (ad.getDoorNo() != null)
				address.setHouse_no(ad.getDoorNo());
			if (ad.getStreet() != null)
				address.setStreet(ad.getStreet());
			if (ad.getLandmark() != null)
				address.setLandmark(ad.getLandmark());
			if (ad.getArea() != null)
				address.setArea(ad.getArea());
			if (ad.getPincode() != null)
				address.setPostalCode(ad.getPincode());

			if (ad.getStateId() != null && ad.getStateId() > 0)
				stateRepository.findById(ad.getStateId()).ifPresent(address::setState);

			if (ad.getCityId() != null && ad.getCityId() > 0)
				cityRepository.findById(ad.getCityId()).ifPresent(address::setCity);

			if (ad.getMandalId() != null && ad.getMandalId() > 0)
				mandalRepository.findById(ad.getMandalId()).ifPresent(address::setMandal);

			if (ad.getDistrictId() != null && ad.getDistrictId() > 0)
				districtRepository.findById(ad.getDistrictId()).ifPresent(address::setDistrict);

			studentAddressRepository.save(address);
		}

		// ================================
		// PAYMENT BLOCK REMOVED AS REQUESTED
		// ================================

		return "Application Sale updated successfully for Admission Number: " + studAdmsNo;
	}

	// Application confirmation - college
	@Transactional
	public String confirmCollegeEnrollment(StudentCollegeConfirmationDto formData) {

		// 1. Fetch main Academic Entity
		StudentAcademicDetails academicDetails = studentAcademicDetailsRepository
				.findByStudAdmsNo(formData.getStudAdmsNo()).orElseThrow(() -> new EntityNotFoundException(
						"Student not found with Admission No: " + formData.getStudAdmsNo()));

		// 2. Update/Set Enrollment Details on StudentAcademicDetails
		// ... (Enrollment update logic remains the same) ...

		// Academic Year
		if (formData.getAcademicYearId() != null)
			academicYearRepository.findById(formData.getAcademicYearId()).ifPresent(academicDetails::setAcademicYear);

		// Joining Class
		if (formData.getJoiningClassId() != null)
			classRepository.findById(formData.getJoiningClassId()).ifPresent(academicDetails::setStudentClass);

		// Branch (Campus)
		if (formData.getBranchId() != null)
			campusRepository.findById(formData.getBranchId()).ifPresent(academicDetails::setCampus);

		// Student Type
		if (formData.getStudentTypeId() != null)
			studentTypeRepository.findById(formData.getStudentTypeId()).ifPresent(academicDetails::setStudentType);

		// City & Course mapping... (Placeholders)

		Status defaultStatus = statusRepository.findById(1)
				.orElseThrow(() -> new EntityNotFoundException("Default Status (ID: 2) not found"));
		academicDetails.setStatus(defaultStatus);

		StudentAcademicDetails savedAcademicDetails = studentAcademicDetailsRepository.save(academicDetails);

		// 3. Save/Update Concession Details (Concession logic remains the same)

		if (formData.getConcessions() != null && !formData.getConcessions().isEmpty()) {
			// ... (Concession UPSERT logic runs here) ...

			Map<Integer, StudentConcessionType> existingConcessionsMap = concessionRepository
					.findByStudAdmsId(savedAcademicDetails.getStud_adms_id()).stream()
					.filter(c -> c.getConcessionType() != null).collect(Collectors.toMap(
							c -> c.getConcessionType().getConcTypeId(), Function.identity(), (first, second) -> first));

			AcademicYear currentYear = savedAcademicDetails.getAcademicYear();
			if (currentYear == null) {
				if (formData.getAcademicYearId() == null) {
					throw new IllegalArgumentException(
							"Academic Year must be provided when no existing year is found.");
				}
				currentYear = academicYearRepository.findById(formData.getAcademicYearId())
						.orElseThrow(() -> new EntityNotFoundException("Academic Year not found"));
			}

			for (ConcessionConfirmationDTO concDto : formData.getConcessions()) {
				StudentConcessionType concession = existingConcessionsMap.get(concDto.getConcessionTypeId());

				if (concession == null) {
					concession = new StudentConcessionType();
					concession.setStudAdmsId(savedAcademicDetails.getStud_adms_id());
					concession.setAcademicYear(currentYear);
					concession.setCreated_by(concDto.getCreatedBy());
					concession.setCreated_Date(LocalDateTime.now());

					if (concDto.getConcessionTypeId() != null) {
						concessionTypeRepository.findById(concDto.getConcessionTypeId())
								.ifPresent(concession::setConcessionType);
					}
				}

				concession.setConc_amount(concDto.getConcessionAmount());
				concession.setComments(concDto.getComments());

				if (concDto.getReasonId() != null) {
					concessionReasonRepository.findById(concDto.getReasonId())
							.ifPresent(concession::setConcessionReason);
				}
				concession.setConc_referred_by(concDto.getConcReferedBy());
				if (concDto.getGivenById() != null) {
					concession.setConc_issued_by(concDto.getGivenById());
				}
				if (concDto.getAuthorizedById() != null) {
					concession.setConc_authorised_by(concDto.getAuthorizedById());
				}

				concessionRepository.save(concession);
			}
		}

		// ==============================================================
		// 🔑 PART 4: CREATE THE PAYMENT AND TRANSACTION (New Logic)
		// ==============================================================
		// ==============================================================
		// 🔑 PART 4: ALWAYS CREATE NEW PAYMENT + TRANSACTION
		// ==============================================================
		PaymentDetailsDTO paymentDTO = formData.getPaymentDetails();

		if (paymentDTO != null && paymentDTO.getAmount() != null) {

			// 🟢 Always create NEW payment record
			PaymentDetails paymentDetails = new PaymentDetails();
			paymentDetails.setStudentAcademicDetails(savedAcademicDetails);
			paymentDetails.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
			paymentDetails.setPre_print_receipt_no(paymentDTO.getPrePrintedReceiptNo());
			paymentDetails.setRemarks(paymentDTO.getRemarks());
			paymentDetails.setCreated_by(paymentDTO.getCreatedBy());
			paymentDetails.setApp_fee(paymentDTO.getAmount());
			paymentDetails.setPaid_amount(paymentDTO.getAmount());
			paymentDetails.setAcedemicYear(savedAcademicDetails.getAcademicYear());
			paymentDetails.setStudentClass(savedAcademicDetails.getStudentClass());

			// Set PaymentMode
			if (paymentDTO.getPaymentModeId() != null) {
				paymentModeRepository.findById(paymentDTO.getPaymentModeId()).ifPresent(paymentDetails::setPaymenMode);
			}

			paymentDetails.setStatus(defaultStatus);
			// 💾 SAVE NEW Payment Record
			PaymentDetails savedPaymentDetails = paymentDetailsRepository.save(paymentDetails);

			// ==============================================================
			// 🔄 TRANSACTION (Only for DD / Cheque — Always NEW Transaction)
			// ==============================================================
			Integer paymentModeId = paymentDTO.getPaymentModeId();
			final int DD_PAYMENT_ID = 2;
			final int CHEQUE_PAYMENT_ID = 3;

			if (paymentModeId != null && (paymentModeId == DD_PAYMENT_ID || paymentModeId == CHEQUE_PAYMENT_ID)) {

				StudentApplicationTransaction transaction = new StudentApplicationTransaction(); // 🔥 Always NEW

				transaction.setPaymentDetails(savedPaymentDetails);
				transaction.setPaymentMode(savedPaymentDetails.getPaymenMode());
				transaction.setNumber(paymentDTO.getTransactionNumber());
				transaction.setDate(paymentDTO.getTransactionDate());
				transaction.setApplication_fee_pay_date(paymentDTO.getPaymentDate());
				transaction.setCreated_by(paymentDTO.getCreatedBy());
				transaction.setStatus("Pending"); // default

				// DD / Cheque extra fields
				transaction.setIfsc_code(paymentDTO.getIfscCode());
				if (paymentDTO.getOrganisationId() != null)
					transaction.setOrg_id(paymentDTO.getOrganisationId());
				if (paymentDTO.getBankId() != null)
					orgBankRepository.findById(paymentDTO.getBankId()).ifPresent(transaction::setOrgBank);
				if (paymentDTO.getBranchId() != null)
					orgBankBranchRepository.findById(paymentDTO.getBranchId()).ifPresent(transaction::setOrgBankBranch);
				if (paymentDTO.getCityId() != null)
					cityRepository.findById(paymentDTO.getCityId()).ifPresent(transaction::setCity);

				// 💾 SAVE NEW Transaction
				studentApplicationTransactionRepository.save(transaction);
			}
		}

		return "College enrollment confirmed, concessions and payment details updated for student: "
				+ formData.getStudAdmsNo();
	}
}