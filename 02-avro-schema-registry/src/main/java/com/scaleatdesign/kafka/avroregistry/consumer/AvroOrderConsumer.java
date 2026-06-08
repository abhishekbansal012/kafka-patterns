package com.scaleatdesign.kafka.avroregistry.consumer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * Consumer that deserializes Avro messages using Schema Registry.
 * Demonstrates type-safe consumption with schema evolution support.
 */
@Slf4j
@Service
public class AvroOrderConsumer {

    @KafkaListener(topics = KafkaTopics.ORDERS_AVRO, groupId = "avro-consumer-group")
    public void consume(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord order = record.value();

        log.info("Consumed Avro order: orderId={}, customerId={}, status={}, partition={}, offset={}",
                order.get("orderId"),
                order.get("customerId"),
                order.get("status"),
                record.partition(),
                record.offset());
    }
}
