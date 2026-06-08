package com.scaleatdesign.kafka.saga.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * Persists saga state for crash recovery and observability.
 */
@Entity
@Table(name = "saga_state", schema = "saga")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaState {

    @Id
    @Column(name = "saga_id")
    private String sagaId;

    @Column(name = "order_id")
    private String orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "current_step")
    private SagaStep currentStep;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private SagaStatus status;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "failure_reason")
    private String failureReason;

    public enum SagaStep {
        INITIATED, PAYMENT, INVENTORY, SHIPPING, COMPLETED, COMPENSATING
    }

    public enum SagaStatus {
        IN_PROGRESS, COMPLETED, FAILED, COMPENSATING, COMPENSATED
    }
}
