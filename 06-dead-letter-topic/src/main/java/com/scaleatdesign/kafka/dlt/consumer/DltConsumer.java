package com.scaleatdesign.kafka.dlt.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

/**
 * DLT Consumer — monitors dead letter topic for failed messages.
 *
 * In production, this would:
 * - Alert operations team
 * - Persist to database for manual review
 * - Trigger automated recovery workflows
 */
@Slf4j
@Service
public class DltConsumer {

    @KafkaListener(
            topics = KafkaTopics.ORDERS_DLT_DEAD,
            groupId = "dlt-monitor-group"
    )
    public void consumeDeadLetter(
            ConsumerRecord<String, ?> record,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String errorMessage,
            @Header(value = KafkaHeaders.ORIGINAL_TOPIC, required = false) String originalTopic,
            @Header(value = KafkaHeaders.ORIGINAL_PARTITION, required = false) Integer originalPartition,
            @Header(value = KafkaHeaders.ORIGINAL_OFFSET, required = false) Long originalOffset
    ) {
        log.error("🚨 DEAD LETTER received:");
        log.error("  Original topic: {}", originalTopic);
        log.error("  Original partition: {}, offset: {}", originalPartition, originalOffset);
        log.error("  Error: {}", errorMessage);
        log.error("  Key: {}", record.key());
        log.error("  Value: {}", record.value());
        log.error("  ACTION REQUIRED: Manual intervention needed");
    }
}
