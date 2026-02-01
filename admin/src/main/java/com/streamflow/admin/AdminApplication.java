package com.streamflow.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring Boot Admin API for StreamFlow
 *
 * Provides REST APIs for:
 * - Topic management (create, list, describe, delete)
 * - Broker health and cluster status
 * - Consumer group monitoring
 * - Metrics collection (Prometheus)
 */
@SpringBootApplication
@EnableScheduling
public class AdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(AdminApplication.class, args);
    }
}
