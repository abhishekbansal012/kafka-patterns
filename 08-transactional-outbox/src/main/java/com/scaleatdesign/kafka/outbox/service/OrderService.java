package com.scaleatdesign.kafka.outbox.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import com.scaleatdesign.kafka.outbox.entity.OrderEntity;
import com.scaleatdesign.kafka.outbox.entity.OutboxEvent;
import com.scaleatdesign.kafka.outbox.repository.OrderRepository;
import com.scaleatdesign.kafka.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Service that writes business entity + outbox event in a SINGLE transaction.
 *
 * This guarantees that:
 * - If the order is saved, the event is also saved (no lost events)
 * - If either fails, both rollback (atomicity)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OrderEntity createOrder(String customerId, String productId, int quantity, java.math.BigDecimal amount) {
        String orderId = UUID.randomUUID().toString();

        // Step 1: Save the business entity
        OrderEntity order = OrderEntity.builder()
                .orderId(orderId)
                .customerId(customerId)
                .productId(productId)
                .quantity(quantity)
                .amount(amount)
                .status("CREATED")
                .createdAt(Instant.now())
                .build();
        orderRepository.save(order);
        log.info("Order saved: {}", orderId);

        // Step 2: Write outbox event (same transaction!)
        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_CREATED")
                .timestamp(Instant.now())
                .source("08-transactional-outbox")
                .orderId(orderId)
                .customerId(customerId)
                .productId(productId)
                .quantity(quantity)
                .amount(amount)
                .status(OrderEvent.OrderStatus.CREATED)
                .build();

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(orderId)
                .aggregateType("ORDER")
                .eventType("ORDER_CREATED")
                .topic(KafkaTopics.OUTBOX_EVENTS)
                .payload(serialize(event))
                .status(OutboxEvent.OutboxStatus.PENDING)
                .createdAt(Instant.now())
                .retryCount(0)
                .build();
        outboxEventRepository.save(outboxEvent);
        log.info("Outbox event saved: aggregateId={}", orderId);

        return order;
    }

    private String serialize(OrderEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }
}
