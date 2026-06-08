package com.scaleatdesign.kafka.producerconsumer.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Binds topic configuration from application.yml.
 *
 * Example:
 *   kafka.topics.orders.name=orders
 *   kafka.topics.orders.partitions=3
 *   kafka.topics.orders.replicas=1
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaTopicProperties {

    private Map<String, TopicConfig> topics;

    @Data
    public static class TopicConfig {
        private String name;
        private int partitions = 1;
        private int replicas = 1;
    }
}
