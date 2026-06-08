package com.scaleatdesign.kafka.retry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 05: Retry Pattern
 *
 * Demonstrates:
 * - @RetryableTopic for automatic retry with exponential backoff
 * - Retry topic naming (topic-retry-0, topic-retry-1, ...)
 * - DLT fallback after max retries exhausted
 * - Non-blocking retry (messages go to retry topics, not blocking the main consumer)
 * - Configurable retry attempts, delays, and backoff multiplier
 */
@SpringBootApplication
public class RetryPatternApplication {

    public static void main(String[] args) {
        SpringApplication.run(RetryPatternApplication.class, args);
    }
}
