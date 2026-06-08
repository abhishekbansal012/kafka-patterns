package com.scaleatdesign.kafka.cqrs.query;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Projector — listens to events and updates the read model.
 * Eventual consistency: read model is updated asynchronously.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProductProjector {

    private final ProductReadModelRepository readModelRepository;

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = KafkaTopics.CQRS_EVENTS, groupId = "cqrs-projector-group")
    public void project(Map<String, Object> event) {
        String eventType = (String) event.get("eventType");
        String productId = (String) event.get("productId");

        log.info("Projecting event: type={}, productId={}", eventType, productId);

        switch (eventType) {
            case "PRODUCT_CREATED" -> {
                BigDecimal price = new BigDecimal(event.get("price").toString());
                int stock = ((Number) event.get("stockQuantity")).intValue();

                ProductReadModel model = ProductReadModel.builder()
                        .productId(productId)
                        .name((String) event.get("name"))
                        .category((String) event.get("category"))
                        .price(price)
                        .stockQuantity(stock)
                        .inStock(stock > 0)
                        .lastUpdated(Instant.now())
                        .build();
                readModelRepository.save(model);
                log.info("Read model created: productId={}", productId);
            }
            case "STOCK_UPDATED" -> {
                readModelRepository.findById(productId).ifPresent(model -> {
                    int change = ((Number) event.get("quantityChange")).intValue();
                    model.setStockQuantity(model.getStockQuantity() + change);
                    model.setInStock(model.getStockQuantity() > 0);
                    model.setLastUpdated(Instant.now());
                    readModelRepository.save(model);
                    log.info("Read model updated: productId={}, newStock={}", productId, model.getStockQuantity());
                });
            }
        }
    }
}
