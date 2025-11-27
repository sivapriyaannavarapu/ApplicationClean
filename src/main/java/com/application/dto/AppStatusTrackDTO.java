package com.application.dto;
 
import lombok.AllArgsConstructor;
 
import lombok.Data;
 
import lombok.NoArgsConstructor;
 
@Data
 
@NoArgsConstructor
 
@AllArgsConstructor
 
public class AppStatusTrackDTO {
 
// ... all 14 fields ...
 
	private Long totalApplications;
 
	private Long appSold;
 
	private Long appConfirmed;
 
	private Long appAvailable;
 
	private Long appIssued;
 
	private Long appDamaged;
 
	private Long appUnavailable;
 
	private Long totalApplicationsLastYear;
 
	private Long appSoldLastYear;
 
	private Long appConfirmedLastYear;
 
	private Long appAvailableLastYear;
 
	private Long appIssuedLastYear;
 
	private Long appDamagedLastYear;
 
	private Long appUnavailableLastYear;
 
	/**
	 *
	 * ADD THIS CONSTRUCTOR: This constructor will match your existing repository
	 * queries.
	 *
	 */
 
	public AppStatusTrackDTO(Long totalApplications, Long appSold, Long appConfirmed, Long appAvailable, Long appIssued,
			Long appDamaged, Long appUnavailable) {
 
		this.totalApplications = totalApplications;
 
		this.appSold = appSold;
 
		this.appConfirmed = appConfirmed;
 
		this.appAvailable = appAvailable;
 
		this.appIssued = appIssued;
 
		this.appDamaged = appDamaged;
 
		this.appUnavailable = appUnavailable;
 
	}
 
}