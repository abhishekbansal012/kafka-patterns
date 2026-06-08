package com.scaleatdesign.kafka.eventsourcing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 10: Event Sourcing
 *
 * Demonstrates:
 * - Append-only event store (no updates, no deletes)
 * - Aggregate reconstruction by replaying events
 * - Kafka as the event bus (events published after persistence)
 * - Snapshot optimization for large event streams
 * - Temporal queries (state at any point in time)
 */
@SpringBootApplication
public class EventSourcingApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventSourcingApplication.class, args);
    }
}
