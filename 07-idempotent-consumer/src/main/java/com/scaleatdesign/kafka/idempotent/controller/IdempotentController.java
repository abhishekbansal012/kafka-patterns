package com.scaleatdesign.kafka.idempotent.controller;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import com.scaleatdesign.kafka.idempotent.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/idempotent")
@RequiredArgsConstructor
public class IdempotentController {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;

    /**
     * Send same message twice to demonstrate deduplication.
     */
    @PostMapping("/duplicate-test")
    public ResponseEntity<Map<String, String>> sendDuplicate() {
        OrderEvent event = buildOrder();

        // Send same event TWICE (simulating at-least-once delivery)
        kafkaTemplate.send(KafkaTopics.ORDERS_IDEMPOTENT, event.getOrderId(), event);
        kafkaTemplate.send(KafkaTopics.ORDERS_IDEMPOTENT, event.getOrderId(), event);

        return ResponseEntity.accepted().body(Map.of(
                "eventId", event.getEventId(),
                "orderId", event.getOrderId(),
                "sentTimes", "2",
                "expectedProcessing", "1 (second is deduplicated)"
        ));
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendOrder() {
        OrderEvent event = buildOrder();
        kafkaTemplate.send(KafkaTopics.ORDERS_IDEMPOTENT, event.getOrderId(), event);
        return ResponseEntity.accepted().body(Map.of("eventId", event.getEventId(), "orderId", event.getOrderId()));
    }

    @GetMapping("/processed-count")
    public ResponseEntity<Map<String, Long>> getProcessedCount() {
        return ResponseEntity.ok(Map.of("processedEvents", processedEventRepository.count()));
    }

    private OrderEvent buildOrder() {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("07-idempotent-consumer")
                .orderId(UUID.randomUUID().toString())
                .customerId("CUST-" + UUID.randomUUID().toString().substring(0, 8))
                .productId("PROD-001")
                .quantity(1)
                .amount(BigDecimal.valueOf(75.00))
                .status(OrderEvent.OrderStatus.CREATED)
                .build();
    }
}
