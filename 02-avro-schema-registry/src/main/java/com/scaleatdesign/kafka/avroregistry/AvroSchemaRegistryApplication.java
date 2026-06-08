package com.scaleatdesign.kafka.avroregistry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 02: Avro Serialization + Confluent Schema Registry
 *
 * Demonstrates:
 * - Avro schema definition (.avsc files)
 * - Schema Registry auto-registration
 * - Avro producer/consumer with type-safe generated classes
 * - Schema evolution (backward/forward compatibility)
 */
@SpringBootApplication
public class AvroSchemaRegistryApplication {

    public static void main(String[] args) {

        SpringApplication.run(AvroSchemaRegistryApplication.class, args);
    }
}
