package com.scaleatdesign.kafka.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module 08: Transactional Outbox Pattern
 *
 * Demonstrates:
 * - Outbox table to guarantee event publishing
 * - Atomic write: business entity + outbox entry in same DB transaction
 * - Polling publisher (scheduler) reads outbox → publishes to Kafka
 * - Handles Kafka downtime gracefully (messages accumulate in outbox)
 * - Ensures exactly-once publishing via status tracking
 *
 * Flow:
 * 1. API creates Order → writes Order + OutboxEvent in single TX
 * 2. Scheduler polls OutboxEvent table every N seconds
 * 3. Publishes pending events to Kafka
 * 4. Marks outbox entries as PUBLISHED
 */
@SpringBootApplication
@EnableScheduling
public class TransactionalOutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(TransactionalOutboxApplication.class, args);
    }
}
