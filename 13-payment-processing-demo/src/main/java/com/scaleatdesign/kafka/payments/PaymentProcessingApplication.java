package com.scaleatdesign.kafka.payments;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module 13: Payment Processing Demo
 *
 * A production-grade payment pipeline combining multiple patterns:
 * - Transactional Outbox (reliable event publishing)
 * - Idempotent Consumer (exactly-once processing)
 * - Retry + DLT (resilient failure handling)
 * - Event-driven notification
 *
 * Flow:
 * 1. Payment Request → Validation → Processing
 * 2. Outbox guarantees event delivery
 * 3. Idempotent processing prevents double-charges
 * 4. Failed payments → DLT for manual review
 * 5. Successful payments → notification event
 */
@SpringBootApplication
@EnableScheduling
public class PaymentProcessingApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentProcessingApplication.class, args);
    }
}
