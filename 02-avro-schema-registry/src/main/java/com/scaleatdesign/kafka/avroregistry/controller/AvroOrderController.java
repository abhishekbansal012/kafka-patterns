package com.scaleatdesign.kafka.avroregistry.controller;

import com.scaleatdesign.kafka.avro.OrderAvro;
import com.scaleatdesign.kafka.avro.OrderStatusAvro;
import com.scaleatdesign.kafka.avroregistry.producer.AvroOrderProducer;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/avro/orders")
@RequiredArgsConstructor
public class AvroOrderController {

    private final AvroOrderProducer producer;

    @PostMapping("/sample")
    public ResponseEntity<Map<String, String>> sendSampleAvroOrder() throws IOException {
        Schema schema = loadSchema();

        GenericRecord order = new GenericData.Record(schema);
        String orderId = UUID.randomUUID().toString();
        order.put("eventId", UUID.randomUUID().toString());
        order.put("eventType", "ORDER_CREATED");
        order.put("timestamp", Instant.now().toEpochMilli());
        order.put("orderId", orderId);
        order.put("customerId", "CUST-" + UUID.randomUUID().toString().substring(0, 8));
        order.put("productId", "PROD-" + UUID.randomUUID().toString().substring(0, 8));
        order.put("quantity", 3);
        order.put("amount", ByteBuffer.wrap(BigDecimal.valueOf(149.99).unscaledValue().toByteArray()));
        order.put("status", new GenericData.EnumSymbol(schema.getField("status").schema(), "CREATED"));

        producer.sendOrder(order);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "serialization", "AVRO",
                "schemaRegistry", "http://localhost:8081"
        ));
    }

    /**
     * SPECIFIC RECORD APPROACH — Compile-time type safety.
     *
     * Uses the generated OrderAvro class (from src/main/avro/order.avsc via Gradle Avro plugin).
     * Benefits:
     * - Typos in field names are caught at compile time (e.g., order.setOrderId() vs order.put("ordreId", ...))
     * - IDE autocompletion and refactoring support
     * - Enum values are type-checked (OrderStatusAvro.CREATED vs raw string "CREATED")
     * - Amount field uses BigDecimal directly instead of manual ByteBuffer conversion
     */
    @PostMapping("/sample-specific")
    public ResponseEntity<Map<String, String>> sendSampleSpecificOrder() {
        String orderId = UUID.randomUUID().toString();

        // Compile-time type safety: every setter is generated from the .avsc schema.
        // If you misspell a field or pass the wrong type, the compiler catches it immediately.
        OrderAvro order = OrderAvro.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("ORDER_CREATED")
                .setTimestamp(Instant.now().toEpochMilli())
                .setOrderId(orderId)
                .setCustomerId("CUST-" + UUID.randomUUID().toString().substring(0, 8))
                .setProductId("PROD-" + UUID.randomUUID().toString().substring(0, 8))
                .setQuantity(3)
                .setAmount(ByteBuffer.wrap(BigDecimal.valueOf(149.99).unscaledValue().toByteArray()))
                .setStatus(OrderStatusAvro.CREATED)  // enum is type-checked at compile time
                .build();

        producer.sendOrder(order);

        return ResponseEntity.accepted().body(Map.of(
                "orderId", orderId,
                "serialization", "AVRO_SPECIFIC",
                "schemaRegistry", "http://localhost:8081"
        ));
    }

    private Schema loadSchema() throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("avro/order.avsc")) {
            return new Schema.Parser().parse(is);
        }
    }
}
