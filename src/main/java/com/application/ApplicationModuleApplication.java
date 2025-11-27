package com.application;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class ApplicationModuleApplication {

	public static void main(String[] args) {
		SpringApplication.run(ApplicationModuleApplication.class, args);
	}

}
