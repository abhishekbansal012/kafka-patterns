package com.scaleatdesign.kafka.producerconsumer.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Kafka topic configuration — driven entirely from application.yml.
 * Partition count and replica factor are externalized, not hardcoded.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaProducerConfig {

    private final KafkaTopicProperties topicProperties;

    @Bean
    public NewTopic ordersTopic() {
        KafkaTopicProperties.TopicConfig config = topicProperties.getTopics().get("orders");
        return TopicBuilder.name(config.getName())
                .partitions(config.getPartitions())
                .replicas(config.getReplicas())
                .build();
    }

    @Bean
    public NewTopic orderConfirmationsTopic() {
        KafkaTopicProperties.TopicConfig config = topicProperties.getTopics().get("order-confirmations");
        return TopicBuilder.name(config.getName())
                .partitions(config.getPartitions())
                .replicas(config.getReplicas())
                .build();
    }
}
