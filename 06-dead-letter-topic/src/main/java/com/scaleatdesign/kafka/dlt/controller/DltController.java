package com.scaleatdesign.kafka.dlt.controller;

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
@RequestMapping("/api/dlt")
@RequiredArgsConstructor
public class DltController {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @PostMapping("/valid")
    public ResponseEntity<Map<String, String>> sendValidOrder() {
        OrderEvent event = buildOrder("CUST-001", BigDecimal.valueOf(49.99));
        kafkaTemplate.send(KafkaTopics.ORDERS_DLT, event.getOrderId(), event);
        return ResponseEntity.accepted().body(Map.of("orderId", event.getOrderId(), "expected", "SUCCESS"));
    }

    @PostMapping("/poison-pill")
    public ResponseEntity<Map<String, String>> sendPoisonPill() {
        OrderEvent event = buildOrder("CUST-001", BigDecimal.valueOf(-1));
        kafkaTemplate.send(KafkaTopics.ORDERS_DLT, event.getOrderId(), event);
        return ResponseEntity.accepted().body(Map.of("orderId", event.getOrderId(), "expected", "DLT after 3 retries"));
    }

    @PostMapping("/missing-customer")
    public ResponseEntity<Map<String, String>> sendMissingCustomer() {
        OrderEvent event = buildOrder("", BigDecimal.valueOf(100));
        kafkaTemplate.send(KafkaTopics.ORDERS_DLT, event.getOrderId(), event);
        return ResponseEntity.accepted().body(Map.of("orderId", event.getOrderId(), "expected", "DLT — missing customer"));
    }

    private OrderEvent buildOrder(String customerId, BigDecimal amount) {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("06-dead-letter-topic")
                .orderId(UUID.randomUUID().toString())
                .customerId(customerId)
                .productId("PROD-001")
                .quantity(1)
                .amount(amount)
                .status(OrderEvent.OrderStatus.CREATED)
                .build();
    }
}
