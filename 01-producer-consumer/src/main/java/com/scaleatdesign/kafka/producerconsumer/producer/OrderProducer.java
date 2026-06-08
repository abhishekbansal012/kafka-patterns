package com.scaleatdesign.kafka.producerconsumer.producer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer service demonstrating:
 * - Async message sending with CompletableFuture callbacks
 * - Key-based routing (orderId as key for ordering guarantee)
 * - Send result logging (partition, offset)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducer {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    /**
     * Send order event asynchronously with callback.
     */
    public CompletableFuture<SendResult<String, OrderEvent>> sendOrder(OrderEvent event) {
        log.info("Producing order event: orderId={}, status={}", event.getOrderId(), event.getStatus());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(KafkaTopics.ORDERS, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order sent successfully: topic={}, partition={}, offset={}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send order: orderId={}", event.getOrderId(), ex);
            }
        });

        return future;
    }

    /**
     * Send order event synchronously (blocking).
     * Useful when you need confirmation before proceeding.
     */
    public SendResult<String, OrderEvent> sendOrderSync(OrderEvent event) throws Exception {
        log.info("Producing order event (sync): orderId={}", event.getOrderId());
        return kafkaTemplate.send(KafkaTopics.ORDERS, event.getOrderId(), event).get();
    }
}
