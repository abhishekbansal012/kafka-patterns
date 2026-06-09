package com.scaleatdesign.kafka.partitioning.config;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.common.event.OrderEvent;
import com.scaleatdesign.kafka.partitioning.partitioner.PriorityPartitioner;
import com.scaleatdesign.kafka.partitioning.partitioner.RegionBasedPartitioner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

@Configuration
public class PartitioningConfig {

    @Bean
    public NewTopic ordersPartitionedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDERS_PARTITIONED)
                .partitions(6) // More partitions to demonstrate routing
                .replicas(1)
                .build();
    }

    /**
     * Custom KafkaTemplate that uses PriorityPartitioner.
     */
    @Bean("priorityPartitionedTemplate")
    public KafkaTemplate<String, OrderEvent> priorityPartitionedTemplate() {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.PARTITIONER_CLASS_CONFIG, PriorityPartitioner.class
        );
        ProducerFactory<String, OrderEvent> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }

    /**
     * Custom KafkaTemplate that uses RegionBasedPartitioner.
     */
    @Bean("regionPartitionedTemplate")
    public KafkaTemplate<String, OrderEvent> regionPartitionedTemplate() {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.PARTITIONER_CLASS_CONFIG, RegionBasedPartitioner.class
        );
        ProducerFactory<String, OrderEvent> factory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(factory);
    }
}
