package com.scaleatdesign.kafka.common.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.math.BigDecimal;

/**
 * A sample order event used across multiple pattern demos.
 */
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class OrderEvent extends BaseEvent {

    private String orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private BigDecimal amount;
    private OrderStatus status;

    public enum OrderStatus {
        CREATED, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, FAILED
    }
}
