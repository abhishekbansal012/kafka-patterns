package com.scaleatdesign.kafka.exactlyonce.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class ExactlyOnceConfig {

    @Bean
    public NewTopic exactlyOnceInputTopic() {
        return TopicBuilder.name(KafkaTopics.EXACTLY_ONCE_INPUT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic exactlyOnceOutputTopic() {
        return TopicBuilder.name(KafkaTopics.EXACTLY_ONCE_OUTPUT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
            ProducerFactory<String, Object> producerFactory) {
        return new KafkaTransactionManager<>(producerFactory);
    }
}
