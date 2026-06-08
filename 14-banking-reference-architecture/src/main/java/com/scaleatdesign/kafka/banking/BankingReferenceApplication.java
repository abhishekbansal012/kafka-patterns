package com.scaleatdesign.kafka.banking;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Module 14: Banking Reference Architecture
 *
 * A comprehensive demo combining ALL Kafka patterns:
 * - Event Sourcing (account event store)
 * - CQRS (separate account write/read models)
 * - Saga (multi-step fund transfer)
 * - Outbox (reliable event publishing)
 * - Exactly-Once (transactional transfer processing)
 * - DLT (failed transfers for manual review)
 *
 * Services simulated within single module:
 * - Account Service: manages accounts via event sourcing
 * - Transfer Service: orchestrates fund transfers via saga
 * - Fraud Service: validates transactions in real-time
 * - Notification Service: sends transfer alerts
 */
@SpringBootApplication
@EnableScheduling
public class BankingReferenceApplication {

    public static void main(String[] args) {
        SpringApplication.run(BankingReferenceApplication.class, args);
    }
}
