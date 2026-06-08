package com.scaleatdesign.kafka.retry.controller;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/retry")
@RequiredArgsConstructor
public class RetryController {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    /**
     * Send an order that will succeed after retries (amount < 500).
     */
    @PostMapping("/recoverable")
    public ResponseEntity<Map<String, String>> sendRecoverableOrder() {
        OrderEvent event = buildOrder(BigDecimal.valueOf(99.99));
        kafkaTemplate.send(KafkaTopics.ORDERS_RETRY, event.getOrderId(), event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", event.getOrderId(),
                "amount", "99.99",
                "expectedBehavior", "Fails 2 times, succeeds on 3rd retry"
        ));
    }

    /**
     * Send an order that will exhaust all retries and end up in DLT (amount > 500).
     */
    @PostMapping("/unrecoverable")
    public ResponseEntity<Map<String, String>> sendUnrecoverableOrder() {
        OrderEvent event = buildOrder(BigDecimal.valueOf(999.99));
        kafkaTemplate.send(KafkaTopics.ORDERS_RETRY, event.getOrderId(), event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", event.getOrderId(),
                "amount", "999.99",
                "expectedBehavior", "Fails all retries → lands in Dead Letter Topic"
        ));
    }

    private OrderEvent buildOrder(BigDecimal amount) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("05-retry-pattern")
                .orderId(UUID.randomUUID().toString())
                .customerId("CUST-" + UUID.randomUUID().toString().substring(0, 8))
                .productId("PROD-001")
                .quantity(1)
                .amount(amount)
                .status(OrderEvent.OrderStatus.CREATED)
                .build();
    }
}
