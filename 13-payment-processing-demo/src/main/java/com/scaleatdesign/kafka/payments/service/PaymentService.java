package com.scaleatdesign.kafka.payments.service;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.payments.entity.Payment;
import com.scaleatdesign.kafka.payments.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public Payment initiatePayment(String orderId, String customerId,
                                    BigDecimal amount, String currency, String idempotencyKey) {
        // Idempotency check
        Optional<Payment> existing = paymentRepository.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            log.warn("Duplicate payment request detected: idempotencyKey={}", idempotencyKey);
            return existing.get();
        }

        Payment payment = Payment.builder()
                .paymentId(UUID.randomUUID().toString())
                .orderId(orderId)
                .customerId(customerId)
                .amount(amount)
                .currency(currency != null ? currency : "USD")
                .status(Payment.PaymentStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .createdAt(Instant.now())
                .build();

        paymentRepository.save(payment);

        // Publish to payment processing topic
        kafkaTemplate.send(KafkaTopics.PAYMENTS, payment.getPaymentId(), Map.of(
                "paymentId", payment.getPaymentId(),
                "orderId", orderId,
                "customerId", customerId,
                "amount", amount,
                "currency", payment.getCurrency(),
                "status", "PENDING"
        ));

        log.info("Payment initiated: paymentId={}, orderId={}, amount={}",
                payment.getPaymentId(), orderId, amount);
        return payment;
    }

    @Transactional
    public void processPayment(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalArgumentException("Payment not found: " + paymentId));

        if (payment.getStatus() != Payment.PaymentStatus.PENDING) {
            log.warn("Payment already processed: {}", paymentId);
            return;
        }

        try {
            // Simulate payment gateway call
            boolean success = simulateGateway(payment);

            if (success) {
                payment.setStatus(Payment.PaymentStatus.COMPLETED);
                payment.setProcessedAt(Instant.now());
                paymentRepository.save(payment);

                // Notify
                kafkaTemplate.send(KafkaTopics.PAYMENT_NOTIFICATIONS, paymentId, Map.of(
                        "paymentId", paymentId, "status", "COMPLETED", "amount", payment.getAmount()));
                log.info("✅ Payment completed: {}", paymentId);
            } else {
                payment.setStatus(Payment.PaymentStatus.FAILED);
                payment.setFailureReason("Gateway declined");
                payment.setProcessedAt(Instant.now());
                paymentRepository.save(payment);
                log.warn("❌ Payment failed: {}", paymentId);
            }
        } catch (Exception e) {
            payment.setStatus(Payment.PaymentStatus.FAILED);
            payment.setFailureReason(e.getMessage());
            paymentRepository.save(payment);
            throw e;
        }
    }

    private boolean simulateGateway(Payment payment) {
        // Simulate: fail if amount > 10000
        return payment.getAmount().compareTo(BigDecimal.valueOf(10000)) < 0;
    }
}
