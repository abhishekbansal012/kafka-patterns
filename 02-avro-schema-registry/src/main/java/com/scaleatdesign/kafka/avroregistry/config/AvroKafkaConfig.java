package com.scaleatdesign.kafka.avroregistry.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class AvroKafkaConfig {

    @Bean
    public NewTopic ordersAvroTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_AVRO)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
