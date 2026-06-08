package com.scaleatdesign.kafka.partitioning.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Custom partitioner that routes messages based on geographic region.
 *
 * Key format expected: "REGION-orderId" (e.g., "US-EAST-order123")
 *
 * Partition mapping:
 * - US-EAST  → Partition 0
 * - US-WEST  → Partition 1
 * - EU       → Partition 2
 * - APAC     → Partition 3
 * - Default  → Partition 4
 *
 * Use case: Data locality, regional compliance, or region-specific consumers.
 */
@Slf4j
public class RegionBasedPartitioner implements Partitioner {

    private static final Map<String, Integer> REGION_PARTITION_MAP = Map.of(
            "US-EAST", 0,
            "US-WEST", 1,
            "EU", 2,
            "APAC", 3
    );

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionsForTopic(topic).size();

        if (key == null) {
            return 0; // Default to partition 0 for null keys
        }

        String keyStr = key.toString();
        for (Map.Entry<String, Integer> entry : REGION_PARTITION_MAP.entrySet()) {
            if (keyStr.startsWith(entry.getKey())) {
                int partition = entry.getValue() % numPartitions;
                log.debug("Routing key '{}' to partition {} (region: {})", key, partition, entry.getKey());
                return partition;
            }
        }

        // Default: hash-based for unknown regions
        int fallback = Math.abs(keyStr.hashCode()) % numPartitions;
        log.debug("Routing key '{}' to partition {} (fallback hash)", key, fallback);
        return fallback;
    }

    @Override
    public void close() {
        // No-op
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No-op
    }
}
