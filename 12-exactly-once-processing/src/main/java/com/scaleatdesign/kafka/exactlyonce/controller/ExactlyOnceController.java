package com.scaleatdesign.kafka.exactlyonce.controller;

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
@RequestMapping("/api/exactly-once")
@RequiredArgsConstructor
public class ExactlyOnceController {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @PostMapping("/produce")
    public ResponseEntity<Map<String, String>> produce() {
        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("12-exactly-once")
                .orderId(UUID.randomUUID().toString())
                .customerId("CUST-EOS")
                .productId("PROD-EOS")
                .quantity(1)
                .amount(BigDecimal.valueOf(200.00))
                .status(OrderEvent.OrderStatus.CREATED)
                .build();

        kafkaTemplate.executeInTransaction(ops -> {
            ops.send(KafkaTopics.EXACTLY_ONCE_INPUT, event.getOrderId(), event);
            return true;
        });

        return ResponseEntity.accepted().body(Map.of(
                "orderId", event.getOrderId(),
                "topic", KafkaTopics.EXACTLY_ONCE_INPUT,
                "note", "Processor will consume, transform, and produce to output topic transactionally"
        ));
    }
}
