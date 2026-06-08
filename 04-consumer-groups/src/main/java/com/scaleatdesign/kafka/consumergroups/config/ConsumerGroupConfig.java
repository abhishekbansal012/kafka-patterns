package com.scaleatdesign.kafka.consumergroups.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.consumergroups.listener.RebalanceLogger;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
public class ConsumerGroupConfig {

    @Bean
    public NewTopic ordersGroupedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_GROUPED)
                .partitions(6) // 6 partitions to demonstrate group distribution
                .replicas(1)
                .build();
    }

    /**
     * Custom container factory with manual ack and rebalance listener.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> manualAckFactory(
            ConsumerFactory<String, Object> consumerFactory,
            RebalanceLogger rebalanceLogger) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3); // 3 consumer threads
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceLogger);
        return factory;
    }
}
