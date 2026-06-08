package com.scaleatdesign.kafka.consumergroups.controller;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/api/consumer-groups")
@RequiredArgsConstructor
public class ConsumerGroupController {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    /**
     * Produce N messages to demonstrate consumer group load balancing.
     */
    @PostMapping("/produce/{count}")
    public ResponseEntity<Map<String, Object>> produceMessages(@PathVariable int count) {
        List<String> orderIds = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            OrderEvent event = OrderEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .eventType("ORDER_CREATED")
                    .timestamp(Instant.now())
                    .source("04-consumer-groups")
                    .orderId(UUID.randomUUID().toString())
                    .customerId("CUST-" + (i % 5)) // 5 different customers
                    .productId("PROD-" + (i % 3))
                    .quantity(i + 1)
                    .amount(BigDecimal.valueOf(10.0 * (i + 1)))
                    .status(OrderEvent.OrderStatus.CREATED)
                    .build();

            kafkaTemplate.send(KafkaTopics.ORDERS_GROUPED, event.getCustomerId(), event);
            orderIds.add(event.getOrderId());
        }

        return ResponseEntity.ok(Map.of(
                "produced", count,
                "topic", KafkaTopics.ORDERS_GROUPED,
                "note", "Watch logs to see messages distributed across consumer threads"
        ));
    }
}
