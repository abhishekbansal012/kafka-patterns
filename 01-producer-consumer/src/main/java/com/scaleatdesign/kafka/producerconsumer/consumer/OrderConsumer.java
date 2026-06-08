package com.scaleatdesign.kafka.producerconsumer.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Kafka Consumer demonstrating:
 * - @KafkaListener annotation-based consumption
 * - Access to message headers (partition, offset, key)
 * - Both typed payload and raw ConsumerRecord approaches
 */
@Slf4j
@Service
public class OrderConsumer {

    /**
     * Approach 1: Typed payload with header extraction.
     * Spring deserializes JSON into OrderEvent automatically.
     */
    @KafkaListener(
            topics = KafkaTopics.ORDERS,
            groupId = "order-consumer-typed"
    )
    public void consumeTyped(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key
    ) {
        log.info("Consumed (typed): orderId={}, status={}, partition={}, offset={}, key={}",
                event.getOrderId(), event.getStatus(), partition, offset, key);

        // Business logic here
        processOrder(event);
    }

    /**
     * Approach 2: Raw ConsumerRecord — gives full control over metadata.
     */
    @KafkaListener(
            topics = KafkaTopics.ORDER_CONFIRMATIONS,
            groupId = "order-consumer-raw"
    )
    public void consumeRaw(ConsumerRecord<String, OrderEvent> record) {
        log.info("Consumed (raw): key={}, value={}, partition={}, offset={}, timestamp={}",
                record.key(), record.value(), record.partition(), record.offset(), record.timestamp());
    }

    private void processOrder(OrderEvent event) {
        // Simulate processing
        log.info("Processing order: {} for customer: {}, amount: {}",
                event.getOrderId(), event.getCustomerId(), event.getAmount());
    }
}
