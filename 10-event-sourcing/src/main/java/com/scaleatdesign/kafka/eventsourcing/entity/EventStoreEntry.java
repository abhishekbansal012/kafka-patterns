package com.scaleatdesign.kafka.eventsourcing.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Append-only event store — every state change is recorded as an event.
 * Never updated or deleted.
 */
@Entity
@Table(name = "event_store", schema = "eventsourcing",
        indexes = @Index(name = "idx_aggregate", columnList = "aggregate_id, version"))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventStoreEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "aggregate_id", nullable = false)
    private String aggregateId;

    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "version", nullable = false)
    private int version;

    @Column(name = "payload", columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Column(name = "metadata", columnDefinition = "TEXT")
    private String metadata;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
