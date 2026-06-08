package com.scaleatdesign.kafka.cqrs.query;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Read model — optimized for queries.
 * Denormalized, possibly with pre-computed fields.
 */
@Entity
@Table(name = "product_read_model", schema = "cqrs")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductReadModel {

    @Id
    @Column(name = "product_id")
    private String productId;

    private String name;
    private String category;
    private BigDecimal price;

    @Column(name = "stock_quantity")
    private int stockQuantity;

    @Column(name = "in_stock")
    private boolean inStock;

    @Column(name = "last_updated")
    private Instant lastUpdated;
}
