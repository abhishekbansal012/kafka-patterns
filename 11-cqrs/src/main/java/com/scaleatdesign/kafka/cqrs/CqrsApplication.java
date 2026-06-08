package com.scaleatdesign.kafka.cqrs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 11: CQRS (Command Query Responsibility Segregation)
 *
 * Demonstrates:
 * - Separate write (command) and read (query) models
 * - Commands → Kafka events → Read model projections
 * - Eventual consistency between command and query sides
 * - Optimized read model for specific query patterns
 * - Projection rebuild capability
 */
@SpringBootApplication
public class CqrsApplication {

    public static void main(String[] args) {
        SpringApplication.run(CqrsApplication.class, args);
    }
}
