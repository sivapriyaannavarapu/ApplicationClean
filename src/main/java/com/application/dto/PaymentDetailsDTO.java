package com.application.dto;
 
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
 
/**
* DTO for payment details and related bank transaction info.
*/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaymentDetailsDTO {
 
    // --- Core Payment Info (Already present) ---
    private Integer paymentModeId;
    private Date paymentDate; // This is the main "Payment Date" from the top of the form
    private Float amount;
    private String prePrintedReceiptNo;
    private String remarks; // This is the "Remarks" field from the Cheque UI
    private Integer createdBy;
    
    // --- New Fields for DD and Cheque Transactions ---
    
    /**
     * Holds the DD Number or Cheque Number.
     */
    private String transactionNumber; // For "DD Number" or a new "Cheque Number" field
    
    /**
     * Holds the DD Date or Cheque Date.
     */
    private Date transactionDate; // For "DD Date" or a new "Cheque Date" field
 
    /**
     * Holds the ID from "Select Organisation Name" (for DD)
     */
    private Integer organisationId;
    
    /**
     * Holds the ID from "Select Bank Name" (for DD and Cheque)
     */
    private Integer bankId;
    
    /**
     * Holds the ID from "Select Branch Name" (for DD and Cheque)
     */
    private Integer branchId;
    
    /**
     * Holds the "IFSC Code" (for Cheque)
     */
    private String ifscCode;
    
    /**
     * Holds the ID from "Select City Name" (for Cheque)
     */
    private Integer cityId;
}
 