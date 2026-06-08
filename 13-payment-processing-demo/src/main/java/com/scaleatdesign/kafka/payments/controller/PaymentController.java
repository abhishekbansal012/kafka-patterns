package com.scaleatdesign.kafka.payments.controller;

import com.scaleatdesign.kafka.payments.entity.Payment;
import com.scaleatdesign.kafka.payments.repository.PaymentRepository;
import com.scaleatdesign.kafka.payments.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;

    @PostMapping
    public ResponseEntity<Payment> initiatePayment(@RequestBody PaymentRequest request) {
        String idempotencyKey = request.idempotencyKey() != null ?
                request.idempotencyKey() : UUID.randomUUID().toString();

        Payment payment = paymentService.initiatePayment(
                request.orderId(), request.customerId(),
                request.amount(), request.currency(), idempotencyKey);

        return ResponseEntity.accepted().body(payment);
    }

    @GetMapping("/{paymentId}")
    public ResponseEntity<Payment> getPayment(@PathVariable String paymentId) {
        return paymentRepository.findById(paymentId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Long>> getStats() {
        long total = paymentRepository.count();
        return ResponseEntity.ok(Map.of("totalPayments", total));
    }

    public record PaymentRequest(
            String orderId, String customerId, BigDecimal amount,
            String currency, String idempotencyKey
    ) {}
}
