package com.scaleatdesign.kafka.idempotent.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Tracks processed event IDs for deduplication.
 * If an event ID already exists in this table, it's a duplicate.
 */
@Entity
@Table(name = "processed_events", schema = "idempotent")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {

    @Id
    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "partition_num")
    private int partition;

    @Column(name = "offset_num")
    private long offset;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    @Column(name = "consumer_group")
    private String consumerGroup;
}
