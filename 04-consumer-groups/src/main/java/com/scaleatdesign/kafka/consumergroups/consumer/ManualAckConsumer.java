package com.scaleatdesign.kafka.consumergroups.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Consumer with manual acknowledgment.
 *
 * Only commits offset AFTER successful processing.
 * If processing fails, message will be re-delivered on next poll.
 * This guarantees at-least-once delivery semantics.
 */
@Slf4j
@Service
public class ManualAckConsumer {

    @KafkaListener(
            topics = KafkaTopics.ORDERS_GROUPED,
            groupId = "order-processing-group",
            containerFactory = "manualAckFactory"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("Consumer [thread={}] received: orderId={}, partition={}, offset={}",
                Thread.currentThread().getName(),
                event.getOrderId(), partition, offset);

        try {
            // Simulate processing
            processOrder(event);

            // Only acknowledge after successful processing
            acknowledgment.acknowledge();
            log.debug("Acknowledged: partition={}, offset={}", partition, offset);
        } catch (Exception e) {
            log.error("Processing failed for orderId={} — NOT acknowledging (will retry)",
                    event.getOrderId(), e);
            // Don't acknowledge — message will be re-delivered
        }
    }

    private void processOrder(OrderEvent event) {
        // Simulate variable processing time
        try {
            Thread.sleep((long) (Math.random() * 100));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Processed order: {} (amount: {})", event.getOrderId(), event.getAmount());
    }
}
