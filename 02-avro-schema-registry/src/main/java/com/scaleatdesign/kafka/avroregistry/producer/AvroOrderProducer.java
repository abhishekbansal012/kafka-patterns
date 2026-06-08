package com.scaleatdesign.kafka.avroregistry.producer;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Producer that sends Avro-serialized messages.
 * Schema is auto-registered with Schema Registry on first send.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AvroOrderProducer {

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

    public void sendOrder(GenericRecord orderAvro) {
        String orderId = orderAvro.get("orderId").toString();
        log.info("Sending Avro order: orderId={}", orderId);

        kafkaTemplate.send(KafkaTopics.ORDERS_AVRO, orderId, orderAvro)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Avro order sent: partition={}, offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to send Avro order: orderId={}", orderId, ex);
                    }
                });
    }
}
