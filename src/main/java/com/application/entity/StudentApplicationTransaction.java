package com.application.entity;
 
import java.util.Date;
 
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
@Table(name = "sce_app_transactions" , schema = "sce_student")
public class StudentApplicationTransaction {
	
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private int app_transaction_id;
	private int org_id;
	private String number;
	private String status;
//	private String cheque_no;
//	private String dd_no;
	private String ifsc_code;
	private Date date;
	private Date application_fee_pay_date;
	private int created_by;
 
	
    @ManyToOne
    @JoinColumn(name = "city_id")
    private City city;	
    
    
    @ManyToOne
    @JoinColumn(name = "org_bank_id")
    private OrgBank orgBank;
    
    
    @ManyToOne
    @JoinColumn(name = "org_bank_branch_id")
    private OrgBankBranch orgBankBranch;
    
    @ManyToOne
	@JoinColumn(name = "payment_mode_id")
	private PaymentMode paymentMode;
    
    @ManyToOne
	@JoinColumn(name = "stud_payment_detls_id")
	private PaymentDetails paymentDetails;
}
 