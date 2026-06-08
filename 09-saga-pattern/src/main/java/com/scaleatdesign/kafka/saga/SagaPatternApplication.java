package com.scaleatdesign.kafka.saga;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 09: Saga Pattern (Orchestrator-based)
 *
 * Demonstrates:
 * - Orchestrator that coordinates multi-step distributed transaction
 * - Compensating transactions on failure
 * - Saga state machine (PENDING → PAYMENT → INVENTORY → SHIPPING → COMPLETED)
 * - Rollback chain when any step fails
 *
 * Flow (Happy Path):
 *   Order Created → Payment Charged → Inventory Reserved → Shipping Scheduled → Order Completed
 *
 * Flow (Failure + Compensation):
 *   Order Created → Payment Charged → Inventory FAILED → Payment Refunded → Order Cancelled
 */
@SpringBootApplication
public class SagaPatternApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaPatternApplication.class, args);
    }
}
