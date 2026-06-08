package com.scaleatdesign.kafka.outbox.controller;

import com.scaleatdesign.kafka.outbox.entity.OrderEntity;
import com.scaleatdesign.kafka.outbox.repository.OutboxEventRepository;
import com.scaleatdesign.kafka.outbox.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/outbox")
@RequiredArgsConstructor
public class OutboxController {

    private final OrderService orderService;
    private final OutboxEventRepository outboxEventRepository;

    @PostMapping("/orders")
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody CreateOrderRequest request) {
        OrderEntity order = orderService.createOrder(
                request.customerId(), request.productId(), request.quantity(), request.amount());

        return ResponseEntity.accepted().body(Map.of(
                "orderId", order.getOrderId(),
                "status", "CREATED",
                "outboxNote", "Event will be published by outbox poller within 2 seconds"
        ));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getOutboxStatus() {
        return ResponseEntity.ok(Map.of(
                "totalEvents", outboxEventRepository.count(),
                "pendingEvents", outboxEventRepository.findPendingEvents().size()
        ));
    }

    public record CreateOrderRequest(String customerId, String productId, int quantity, BigDecimal amount) {}
}
