package com.scaleatdesign.kafka.outbox.publisher;

import com.scaleatdesign.kafka.outbox.entity.OutboxEvent;
import com.scaleatdesign.kafka.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Polling publisher that reads pending outbox events and publishes them to Kafka.
 *
 * Runs on a fixed interval (configurable via application.yml).
 * Marks events as PUBLISHED or FAILED after attempting.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:2000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findPendingEvents();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Outbox publisher found {} pending events", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(); // Blocking send for reliability

                event.setStatus(OutboxEvent.OutboxStatus.PUBLISHED);
                event.setPublishedAt(Instant.now());
                outboxEventRepository.save(event);

                log.info("Published outbox event: id={}, aggregateId={}, topic={}",
                        event.getId(), event.getAggregateId(), event.getTopic());
            } catch (Exception e) {
                event.setRetryCount(event.getRetryCount() + 1);
                if (event.getRetryCount() > 5) {
                    event.setStatus(OutboxEvent.OutboxStatus.FAILED);
                    log.error("Outbox event FAILED after max retries: id={}", event.getId(), e);
                } else {
                    log.warn("Outbox publish failed (retry {}): id={}", event.getRetryCount(), event.getId(), e);
                }
                outboxEventRepository.save(event);
            }
        }
    }
}
