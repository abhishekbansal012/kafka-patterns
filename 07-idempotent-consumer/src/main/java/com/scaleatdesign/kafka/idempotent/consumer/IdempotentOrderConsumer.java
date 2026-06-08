package com.scaleatdesign.kafka.idempotent.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import com.scaleatdesign.kafka.idempotent.entity.ProcessedEvent;
import com.scaleatdesign.kafka.idempotent.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Idempotent consumer that deduplicates messages using event ID.
 *
 * Algorithm:
 * 1. Check if eventId exists in processed_events table
 * 2. If YES → skip (duplicate), acknowledge
 * 3. If NO → process, save to processed_events, acknowledge
 *
 * This converts at-least-once into effectively-once processing.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotentOrderConsumer {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    @KafkaListener(topics = KafkaTopics.ORDERS_IDEMPOTENT, groupId = "idempotent-consumer-group")
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        String eventId = event.getEventId();

        // Step 1: Deduplication check
        if (processedEventRepository.existsById(eventId)) {
            log.warn("⏭️ DUPLICATE detected — skipping eventId={}, orderId={}",
                    eventId, event.getOrderId());
            acknowledgment.acknowledge();
            return;
        }

        // Step 2: Process the message
        log.info("Processing new order: eventId={}, orderId={}, amount={}",
                eventId, event.getOrderId(), event.getAmount());
        processOrder(event);

        // Step 3: Record as processed (within same transaction)
        ProcessedEvent record = ProcessedEvent.builder()
                .eventId(eventId)
                .topic(topic)
                .partition(partition)
                .offset(offset)
                .processedAt(Instant.now())
                .consumerGroup("idempotent-consumer-group")
                .build();
        processedEventRepository.save(record);

        // Step 4: Acknowledge
        acknowledgment.acknowledge();
        log.info("✅ Order processed and recorded: eventId={}", eventId);
    }

    private void processOrder(OrderEvent event) {
        // Simulate business logic
        log.info("Business logic executed for order: {} (customer: {}, amount: {})",
                event.getOrderId(), event.getCustomerId(), event.getAmount());
    }
}
