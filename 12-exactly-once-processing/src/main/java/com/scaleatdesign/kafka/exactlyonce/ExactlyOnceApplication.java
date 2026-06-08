package com.scaleatdesign.kafka.exactlyonce;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 12: Exactly-Once Semantics (EOS)
 *
 * Demonstrates:
 * - Kafka Transactions (producer transactional.id)
 * - read_committed isolation level on consumers
 * - Consume-transform-produce (CTP) pattern atomically
 * - KafkaTransactionManager with Spring
 * - idempotent producer (enable.idempotence=true)
 */
@SpringBootApplication
public class ExactlyOnceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExactlyOnceApplication.class, args);
    }
}
