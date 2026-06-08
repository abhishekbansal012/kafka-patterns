package com.scaleatdesign.kafka.cqrs.command;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateProductCommand {
    private String productId;
    private String name;
    private String category;
    private BigDecimal price;
    private int stockQuantity;
}
