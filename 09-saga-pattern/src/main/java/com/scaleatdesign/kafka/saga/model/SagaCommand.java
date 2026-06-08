package com.scaleatdesign.kafka.saga.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * Command message sent by orchestrator to saga participants.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaCommand {

    private String sagaId;
    private String orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal amount;
    private CommandType commandType;
    private String correlationId;

    public enum CommandType {
        // Forward commands
        PROCESS_PAYMENT,
        RESERVE_INVENTORY,
        SCHEDULE_SHIPPING,

        // Compensating commands (rollback)
        REFUND_PAYMENT,
        RELEASE_INVENTORY,
        CANCEL_SHIPPING
    }
}
