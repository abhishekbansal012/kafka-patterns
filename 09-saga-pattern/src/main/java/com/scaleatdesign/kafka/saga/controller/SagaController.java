package com.scaleatdesign.kafka.saga.controller;

import com.scaleatdesign.kafka.saga.entity.SagaState;
import com.scaleatdesign.kafka.saga.orchestrator.OrderSagaOrchestrator;
import com.scaleatdesign.kafka.saga.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/saga")
@RequiredArgsConstructor
public class SagaController {

    private final OrderSagaOrchestrator orchestrator;
    private final SagaStateRepository sagaStateRepository;

    /**
     * Start a happy-path saga (amount=100, quantity=2 → succeeds all steps).
     */
    @PostMapping("/happy-path")
    public ResponseEntity<Map<String, String>> happyPath() {
        String orderId = UUID.randomUUID().toString();
        String sagaId = orchestrator.startSaga(orderId, "CUST-001", "PROD-001", 2, BigDecimal.valueOf(100));
        return ResponseEntity.accepted().body(Map.of("sagaId", sagaId, "orderId", orderId, "expected", "COMPLETED"));
    }

    /**
     * Start a failing saga (quantity=15 → inventory fails → payment compensated).
     */
    @PostMapping("/inventory-failure")
    public ResponseEntity<Map<String, String>> inventoryFailure() {
        String orderId = UUID.randomUUID().toString();
        String sagaId = orchestrator.startSaga(orderId, "CUST-002", "PROD-002", 15, BigDecimal.valueOf(200));
        return ResponseEntity.accepted().body(Map.of("sagaId", sagaId, "orderId", orderId, "expected", "COMPENSATED (inventory failure)"));
    }

    /**
     * Start a failing saga (amount=2000 → payment fails).
     */
    @PostMapping("/payment-failure")
    public ResponseEntity<Map<String, String>> paymentFailure() {
        String orderId = UUID.randomUUID().toString();
        String sagaId = orchestrator.startSaga(orderId, "CUST-003", "PROD-003", 1, BigDecimal.valueOf(2000));
        return ResponseEntity.accepted().body(Map.of("sagaId", sagaId, "orderId", orderId, "expected", "COMPENSATED (payment failure)"));
    }

    @GetMapping("/{sagaId}")
    public ResponseEntity<SagaState> getSagaStatus(@PathVariable String sagaId) {
        return sagaStateRepository.findById(sagaId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
