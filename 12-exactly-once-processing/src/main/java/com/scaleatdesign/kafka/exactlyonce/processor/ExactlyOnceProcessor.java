package com.scaleatdesign.kafka.exactlyonce.processor;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Exactly-once consume-transform-produce (CTP) processor.
 *
 * Key guarantees:
 * - Consumer reads ONLY committed messages (isolation.level=read_committed)
 * - Producer writes are transactional (transactional.id prefix set)
 * - Consumer offset commit is part of the same transaction
 *
 * If any step fails, the entire transaction rolls back:
 * - The consumed message offset is NOT committed
 * - The produced output message is NOT visible to read_committed consumers
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExactlyOnceProcessor {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional("kafkaTransactionManager")
    @KafkaListener(topics = KafkaTopics.EXACTLY_ONCE_INPUT, groupId = "exactly-once-group")
    public void processExactlyOnce(OrderEvent input) {
        log.info("EOS: Processing input order: {} (amount: {})", input.getOrderId(), input.getAmount());

        // Transform: enrich the order
        BigDecimal tax = input.getAmount().multiply(BigDecimal.valueOf(0.1));
        BigDecimal totalWithTax = input.getAmount().add(tax);

        Map<String, Object> enrichedOutput = Map.of(
                "eventId", UUID.randomUUID().toString(),
                "originalOrderId", input.getOrderId(),
                "originalAmount", input.getAmount(),
                "tax", tax,
                "totalWithTax", totalWithTax,
                "processedAt", Instant.now().toString(),
                "processor", "exactly-once-eos"
        );

        // Produce: write to output topic (within same transaction)
        kafkaTemplate.send(KafkaTopics.EXACTLY_ONCE_OUTPUT, input.getOrderId(), enrichedOutput);

        log.info("EOS: Output produced — orderId={}, total={} (original={}, tax={})",
                input.getOrderId(), totalWithTax, input.getAmount(), tax);
    }
}
