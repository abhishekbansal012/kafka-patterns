package com.scaleatdesign.kafka.dlt.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Configuration for DLT pattern:
 * - Retry 3 times with 1-second backoff
 * - After exhaustion, publish to .DLT topic
 */
@Configuration
public class DltConfig {

    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic ordersDltDeadTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_DLT_DEAD)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Custom error handler that publishes to DLT after retries exhausted.
     * Adds error headers: exception class, message, stack trace, original topic.
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // Custom recoverer that routes to topic + ".DLT"
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception ex) -> {
                    // Route to .DLT suffix topic
                    return new org.apache.kafka.common.TopicPartition(
                            record.topic() + ".DLT", record.partition());
                }
        );

        // 3 retries, 1 second between each
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3));

        // Don't retry on deserialization errors — straight to DLT
        handler.addNotRetryableExceptions(org.apache.kafka.common.errors.SerializationException.class);

        return handler;
    }
}
