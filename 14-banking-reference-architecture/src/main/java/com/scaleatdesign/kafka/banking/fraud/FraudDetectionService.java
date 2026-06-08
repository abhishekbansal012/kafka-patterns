package com.scaleatdesign.kafka.banking.fraud;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Real-time fraud detection service.
 * Checks transfers against rules and publishes fraud events.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FraudDetectionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // Simple rule-based fraud detection
    private static final BigDecimal SUSPICIOUS_THRESHOLD = BigDecimal.valueOf(50000);

    public void checkFraud(String transferId, String fromAccount, String toAccount, BigDecimal amount) {
        log.info("Fraud check: transferId={}, amount={}", transferId, amount);

        if (amount.compareTo(SUSPICIOUS_THRESHOLD) > 0) {
            kafkaTemplate.send(KafkaTopics.BANK_FRAUD, transferId, Map.of(
                    "type", "SUSPICIOUS_TRANSFER",
                    "transferId", transferId,
                    "fromAccount", fromAccount,
                    "toAccount", toAccount,
                    "amount", amount,
                    "reason", "Amount exceeds threshold of " + SUSPICIOUS_THRESHOLD,
                    "timestamp", Instant.now().toString()
            ));

            throw new IllegalStateException("Transfer flagged as suspicious: amount=" + amount +
                    " exceeds threshold=" + SUSPICIOUS_THRESHOLD);
        }

        if (fromAccount.equals(toAccount)) {
            throw new IllegalArgumentException("Self-transfer not allowed");
        }

        log.info("Fraud check PASSED: transferId={}", transferId);
    }
}
