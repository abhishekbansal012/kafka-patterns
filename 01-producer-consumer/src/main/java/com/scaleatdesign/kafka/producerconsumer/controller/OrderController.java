package com.scaleatdesign.kafka.producerconsumer.controller;

import com.scaleatdesign.kafka.common.event.OrderEvent;
import com.scaleatdesign.kafka.producerconsumer.producer.OrderProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * REST controller to trigger Kafka message production.
 * Useful for testing and demonstration.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducer orderProducer;

    /**
     * POST /api/orders — Create and publish an order event.
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody CreateOrderRequest request) {
        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("01-producer-consumer")
                .orderId(UUID.randomUUID().toString())
                .customerId(request.customerId())
                .productId(request.productId())
                .quantity(request.quantity())
                .amount(request.amount())
                .status(OrderEvent.OrderStatus.CREATED)
                .build();

        orderProducer.sendOrder(event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", event.getOrderId(),
                "status", "ACCEPTED"
        ));
    }

    /**
     * POST /api/orders/sample — Generate a sample order for quick testing.
     */
    @PostMapping("/sample")
    public ResponseEntity<Map<String, String>> createSampleOrder() {
        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("01-producer-consumer")
                .orderId(UUID.randomUUID().toString())
                .customerId("CUST-" + UUID.randomUUID().toString().substring(0, 8))
                .productId("PROD-" + UUID.randomUUID().toString().substring(0, 8))
                .quantity(2)
                .amount(BigDecimal.valueOf(99.99))
                .status(OrderEvent.OrderStatus.CREATED)
                .build();

        orderProducer.sendOrder(event);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", event.getOrderId(),
                "status", "ACCEPTED",
                "message", "Sample order created and sent to Kafka"
        ));
    }

    public record CreateOrderRequest(
            String customerId,
            String productId,
            int quantity,
            BigDecimal amount
    ) {}
}
