package com.scaleatdesign.kafka.consumergroups.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Logs partition assignment/revocation during consumer group rebalancing.
 * Critical for understanding consumer group behavior in production.
 *
 * Rebalancing happens when:
 * - A new consumer joins the group
 * - An existing consumer leaves (crash or shutdown)
 * - Partitions are added to the topic
 * - Consumer group coordinator detects session timeout
 */
@Slf4j
@Component
public class RebalanceLogger implements ConsumerRebalanceListener {

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            log.warn("⚠️  PARTITIONS REVOKED: {} — Finish processing before handoff!",
                    formatPartitions(partitions));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!partitions.isEmpty()) {
            log.info("✅ PARTITIONS ASSIGNED: {} — Ready to consume",
                    formatPartitions(partitions));
        }
    }

    /**
     * Formats a collection of TopicPartition objects into a readable log string.
     * Example: {TopicPartition(orders-grouped, 0), TopicPartition(orders-grouped, 2)}
     *        → "[orders-grouped-0, orders-grouped-2]"
     */
    private String formatPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream()
                .map(tp -> tp.topic() + "-" + tp.partition())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
