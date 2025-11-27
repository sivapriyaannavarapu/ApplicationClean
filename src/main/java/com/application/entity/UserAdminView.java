package com.application.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "sce_user_admin" , schema = "sce_admin")
public class UserAdminView {

	@Id
	private int user_role_id;
	private int emp_id;
	private String first_name;
	private String last_name;
	private String role_name;
	private String designation_name;
}
