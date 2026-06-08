package com.scaleatdesign.kafka.cqrs.controller;

import com.scaleatdesign.kafka.cqrs.command.CommandHandler;
import com.scaleatdesign.kafka.cqrs.command.CreateProductCommand;
import com.scaleatdesign.kafka.cqrs.query.ProductReadModel;
import com.scaleatdesign.kafka.cqrs.query.ProductReadModelRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/cqrs/products")
@RequiredArgsConstructor
public class CqrsController {

    private final CommandHandler commandHandler;
    private final ProductReadModelRepository readModelRepository;

    // === COMMAND SIDE ===

    @PostMapping
    public ResponseEntity<Map<String, String>> createProduct(@RequestBody CreateProductCommand command) {
        String productId = commandHandler.handleCreateProduct(command);
        return ResponseEntity.accepted().body(Map.of(
                "productId", productId,
                "note", "Event published — read model updates asynchronously"
        ));
    }

    @PatchMapping("/{productId}/stock")
    public ResponseEntity<Map<String, String>> updateStock(@PathVariable String productId, @RequestBody Map<String, Integer> body) {
        commandHandler.handleUpdateStock(productId, body.get("quantityChange"));
        return ResponseEntity.accepted().body(Map.of("productId", productId, "status", "STOCK_UPDATE_PUBLISHED"));
    }

    // === QUERY SIDE ===

    @GetMapping
    public ResponseEntity<List<ProductReadModel>> getAllProducts() {
        return ResponseEntity.ok(readModelRepository.findAll());
    }

    @GetMapping("/{productId}")
    public ResponseEntity<ProductReadModel> getProduct(@PathVariable String productId) {
        return readModelRepository.findById(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/in-stock")
    public ResponseEntity<List<ProductReadModel>> getInStock() {
        return ResponseEntity.ok(readModelRepository.findByInStockTrue());
    }

    @GetMapping("/category/{category}")
    public ResponseEntity<List<ProductReadModel>> getByCategory(@PathVariable String category) {
        return ResponseEntity.ok(readModelRepository.findByCategory(category));
    }
}
