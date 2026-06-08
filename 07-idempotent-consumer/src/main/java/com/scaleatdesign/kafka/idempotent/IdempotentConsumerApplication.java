package com.scaleatdesign.kafka.idempotent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 07: Idempotent Consumer Pattern
 *
 * Demonstrates:
 * - Message deduplication using event ID stored in database
 * - Exactly-once semantics from at-least-once delivery
 * - Idempotency key tracking table
 * - Transactional processing (DB + Kafka ack in same transaction)
 * - Duplicate detection and graceful skip
 */
@SpringBootApplication
public class IdempotentConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(IdempotentConsumerApplication.class, args);
    }
}
