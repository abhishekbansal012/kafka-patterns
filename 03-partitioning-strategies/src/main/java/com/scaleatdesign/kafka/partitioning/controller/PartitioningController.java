package com.scaleatdesign.kafka.partitioning.controller;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/partitioning")
@RequiredArgsConstructor
public class PartitioningController {

    @Qualifier("regionPartitionedTemplate")
    private final KafkaTemplate<String, OrderEvent> regionTemplate;

    private final KafkaTemplate<String, OrderEvent> defaultTemplate;

    /**
     * Send messages using region-based partitioning.
     * Messages with same region prefix go to same partition.
     */
    @PostMapping("/region")
    public ResponseEntity<Map<String, Object>> sendByRegion() {
        List<String> regions = List.of("US-EAST", "US-WEST", "EU", "APAC");
        List<Map<String, String>> sent = new ArrayList<>();

        for (String region : regions) {
            OrderEvent event = buildSampleOrder();
            String key = region + "-" + event.getOrderId();

            regionTemplate.send(KafkaTopics.ORDERS_PARTITIONED, key, event)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Region {} → partition {}", region, result.getRecordMetadata().partition());
                        }
                    });

            sent.add(Map.of("region", region, "key", key, "orderId", event.getOrderId()));
        }

        return ResponseEntity.ok(Map.of(
                "strategy", "REGION_BASED",
                "messages", sent,
                "note", "Check logs to see partition assignments per region"
        ));
    }

    /**
     * Send messages with explicit partition selection.
     * Bypasses any partitioner — direct partition control.
     */
    @PostMapping("/explicit/{partition}")
    public ResponseEntity<Map<String, String>> sendToPartition(@PathVariable int partition) {
        OrderEvent event = buildSampleOrder();

        defaultTemplate.send(KafkaTopics.ORDERS_PARTITIONED, partition, event.getOrderId(), event);

        return ResponseEntity.ok(Map.of(
                "strategy", "EXPLICIT",
                "partition", String.valueOf(partition),
                "orderId", event.getOrderId()
        ));
    }

    /**
     * Send messages demonstrating key-based ordering guarantee.
     * Same customerId always goes to same partition → ordering preserved.
     */
    @PostMapping("/key-ordering")
    public ResponseEntity<Map<String, Object>> sendWithKeyOrdering() {
        String customerId = "CUST-FIXED-" + UUID.randomUUID().toString().substring(0, 4);
        List<String> orderIds = new ArrayList<>();

        // 5 orders for same customer — all should land on same partition
        for (int i = 0; i < 5; i++) {
            OrderEvent event = buildSampleOrder();
            event.setCustomerId(customerId);
            String key = customerId; // Same key = same partition

            defaultTemplate.send(KafkaTopics.ORDERS_PARTITIONED, key, event)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            log.info("Key {} → partition {} (offset {})",
                                    customerId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                        }
                    });
            orderIds.add(event.getOrderId());
        }

        return ResponseEntity.ok(Map.of(
                "strategy", "KEY_BASED_ORDERING",
                "customerId", customerId,
                "orderIds", orderIds,
                "note", "All 5 orders for same customer go to same partition, preserving order"
        ));
    }

    private OrderEvent buildSampleOrder() {
        return OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("03-partitioning-strategies")
                .orderId(UUID.randomUUID().toString())
                .customerId("CUST-" + UUID.randomUUID().toString().substring(0, 8))
                .productId("PROD-" + UUID.randomUUID().toString().substring(0, 8))
                .quantity(1)
                .amount(BigDecimal.valueOf(59.99))
                .status(OrderEvent.OrderStatus.CREATED)
                .build();
    }
}
