package com.scaleatdesign.kafka.partitioning;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 03: Partitioning Strategies
 *
 * Demonstrates:
 * - Default partitioning (murmur2 hash on key)
 * - Custom partitioner implementations
 * - Round-robin for even distribution
 * - Region-based partitioning for data locality
 * - How partition assignment affects message ordering
 */
@SpringBootApplication
public class PartitioningApplication {

    public static void main(String[] args) {
        SpringApplication.run(PartitioningApplication.class, args);
    }
}
