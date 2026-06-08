package com.scaleatdesign.kafka.eventsourcing.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.eventsourcing.aggregate.AccountAggregate;
import com.scaleatdesign.kafka.eventsourcing.entity.EventStoreEntry;
import com.scaleatdesign.kafka.eventsourcing.repository.EventStoreRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventStoreService {

    private final EventStoreRepository eventStoreRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Append a new event to the store and publish to Kafka.
     */
    @Transactional
    public EventStoreEntry appendEvent(String aggregateId, String aggregateType,
                                        String eventType, Map<String, Object> payload) {
        Integer currentVersion = eventStoreRepository.findMaxVersionByAggregateId(aggregateId);
        int nextVersion = (currentVersion == null) ? 1 : currentVersion + 1;

        String payloadJson = serialize(payload);

        EventStoreEntry entry = EventStoreEntry.builder()
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .eventType(eventType)
                .version(nextVersion)
                .payload(payloadJson)
                .createdAt(Instant.now())
                .build();

        eventStoreRepository.save(entry);
        log.info("Event stored: aggregateId={}, type={}, version={}", aggregateId, eventType, nextVersion);

        // Publish to Kafka for other services
        kafkaTemplate.send(KafkaTopics.EVENT_STORE, aggregateId, payloadJson);

        return entry;
    }

    /**
     * Reconstruct aggregate by replaying all events.
     */
    public AccountAggregate loadAggregate(String aggregateId) {
        List<EventStoreEntry> events = eventStoreRepository.findByAggregateIdOrderByVersionAsc(aggregateId);

        AccountAggregate aggregate = new AccountAggregate();
        for (EventStoreEntry event : events) {
            Map<String, Object> payload = deserialize(event.getPayload());
            aggregate.apply(event.getEventType(), payload);
        }

        log.info("Aggregate loaded: id={}, version={}, balance={}", aggregateId, aggregate.getVersion(), aggregate.getBalance());
        return aggregate;
    }

    private String serialize(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private Map<String, Object> deserialize(String json) {
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Deserialization failed", e);
        }
    }
}
