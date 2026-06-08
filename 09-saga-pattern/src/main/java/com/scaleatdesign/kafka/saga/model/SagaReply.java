package com.scaleatdesign.kafka.saga.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reply from saga participant back to orchestrator.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaReply {

    private String sagaId;
    private String orderId;
    private String participantName;
    private ReplyStatus status;
    private String message;
    private String correlationId;

    public enum ReplyStatus {
        SUCCESS, FAILURE
    }
}
