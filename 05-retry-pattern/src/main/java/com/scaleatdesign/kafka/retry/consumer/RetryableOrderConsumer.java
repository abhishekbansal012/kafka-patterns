package com.scaleatdesign.kafka.retry.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

/**
 * Demonstrates Spring Kafka's @RetryableTopic — non-blocking retry pattern.
 *
 * Flow:
 * 1. Message arrives on 'orders-retry' topic
 * 2. If processing fails → message moves to 'orders-retry-retry-0' (wait 1s)
 * 3. Retry fails → 'orders-retry-retry-1' (wait 2s)
 * 4. Retry fails → 'orders-retry-retry-2' (wait 4s)
 * 5. All retries exhausted → 'orders-retry-dlt' (dead letter topic)
 *
 * Key advantage: Main consumer is NOT blocked during retries.
 */
@Slf4j
@Service
public class RetryableOrderConsumer {

    private int attemptCounter = 0;

    @RetryableTopic(
            attempts = "4",           // 1 original + 3 retries
            backoff = @Backoff(
                    delay = 1000,     // Initial delay: 1 second
                    multiplier = 2,   // Exponential: 1s → 2s → 4s
                    maxDelay = 10000  // Cap at 10 seconds
            ),
            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR,
            autoCreateTopics = "true",
            include = {RuntimeException.class} // Only retry on RuntimeExceptions
    )
    @KafkaListener(topics = KafkaTopics.ORDERS_RETRY, groupId = "retry-consumer-group")
    public void consume(
            OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String errorMessage
    ) {
        attemptCounter++;
        log.info("Attempt #{} — Processing order: {} from topic: {}",
                attemptCounter, event.getOrderId(), topic);

        // Simulate intermittent failure (fails first 2 attempts, succeeds on 3rd)
        if (shouldSimulateFailure(event)) {
            log.warn("Simulated transient failure for order: {}", event.getOrderId());
            throw new RuntimeException("Transient service unavailable — order: " + event.getOrderId());
        }

        log.info("✅ Successfully processed order: {} after retries", event.getOrderId());
        attemptCounter = 0;
    }

    /**
     * Dead Letter Topic handler — invoked when all retries are exhausted.
     * This is where you'd alert, persist for manual review, etc.
     */
    @DltHandler
    public void handleDlt(
            OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String errorMessage
    ) {
        log.error("❌ DLT HANDLER: Order {} exhausted all retries. Topic: {}, Error: {}",
                event.getOrderId(), topic, errorMessage);
        log.error("Action required: Manual intervention for order {}", event.getOrderId());
        attemptCounter = 0;
    }

    /**
     * Simulate failure for orders with amount > 500 (always fail → goes to DLT)
     * Orders with amount < 500 fail twice then succeed on 3rd try.
     */
    private boolean shouldSimulateFailure(OrderEvent event) {
        if (event.getAmount() != null && event.getAmount().doubleValue() > 500) {
            return true; // Always fail → ends up in DLT
        }
        return attemptCounter < 3; // Fail first 2 times, succeed on 3rd
    }
}
