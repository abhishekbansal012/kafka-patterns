package com.scaleatdesign.kafka.idempotent.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class IdempotentConfig {

    @Bean
    public NewTopic ordersIdempotentTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_IDEMPOTENT)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
