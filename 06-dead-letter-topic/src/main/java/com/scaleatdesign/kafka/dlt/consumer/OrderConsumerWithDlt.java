package com.scaleatdesign.kafka.dlt.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Main consumer that may fail — failed messages go to DLT.
 */
@Slf4j
@Service
public class OrderConsumerWithDlt {

    @KafkaListener(topics = KafkaTopics.ORDERS_DLT, groupId = "dlt-consumer-group")
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Processing order: {} (amount: {})", event.getOrderId(), event.getAmount());

        // Simulate failure for "poison pill" messages
        if (event.getAmount() != null && event.getAmount().doubleValue() < 0) {
            throw new IllegalArgumentException("Invalid order amount: " + event.getAmount());
        }

        if (event.getCustomerId() == null || event.getCustomerId().isBlank()) {
            throw new IllegalStateException("Missing customer ID for order: " + event.getOrderId());
        }

        log.info("✅ Order processed successfully: {}", event.getOrderId());
    }
}
