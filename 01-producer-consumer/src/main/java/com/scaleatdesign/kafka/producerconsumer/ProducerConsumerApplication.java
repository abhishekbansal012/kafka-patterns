package com.scaleatdesign.kafka.producerconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 01: Basic Producer-Consumer Pattern
 *
 * Demonstrates:
 * - KafkaTemplate for producing messages
 * - @KafkaListener for consuming messages
 * - JSON serialization/deserialization
 * - REST API trigger for producing
 * - Callback-based send confirmation
 */
@SpringBootApplication
public class ProducerConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerConsumerApplication.class, args);
    }
}
