package com.scaleatdesign.kafka.cqrs.command;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Command side — validates and publishes domain events.
 * Does NOT update read model directly.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CommandHandler {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public String handleCreateProduct(CreateProductCommand command) {
        // Validate
        if (command.getPrice() == null || command.getPrice().doubleValue() < 0) {
            throw new IllegalArgumentException("Invalid price");
        }

        String productId = command.getProductId() != null ?
                command.getProductId() : UUID.randomUUID().toString();

        // Publish event (not direct DB write)
        Map<String, Object> event = Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "PRODUCT_CREATED",
                "timestamp", Instant.now().toString(),
                "productId", productId,
                "name", command.getName(),
                "category", command.getCategory(),
                "price", command.getPrice(),
                "stockQuantity", command.getStockQuantity()
        );

        kafkaTemplate.send(KafkaTopics.CQRS_EVENTS, productId, event);
        log.info("Command handled: PRODUCT_CREATED → event published for productId={}", productId);

        return productId;
    }

    public void handleUpdateStock(String productId, int quantityChange) {
        Map<String, Object> event = Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "STOCK_UPDATED",
                "timestamp", Instant.now().toString(),
                "productId", productId,
                "quantityChange", quantityChange
        );

        kafkaTemplate.send(KafkaTopics.CQRS_EVENTS, productId, event);
        log.info("Command handled: STOCK_UPDATED → productId={}, change={}", productId, quantityChange);
    }
}
