package com.scaleatdesign.kafka.dlt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 06: Dead Letter Topic (DLT) Pattern
 *
 * Demonstrates:
 * - DeadLetterPublishingRecoverer for routing failed messages
 * - Custom DLT naming and configuration
 * - DLT consumer for monitoring/alerting
 * - Error headers (original topic, exception, timestamp)
 * - Poison pill handling strategy
 * - Reprocessing DLT messages after fix
 */
@SpringBootApplication
public class DeadLetterTopicApplication {

    public static void main(String[] args) {
        SpringApplication.run(DeadLetterTopicApplication.class, args);
    }
}
