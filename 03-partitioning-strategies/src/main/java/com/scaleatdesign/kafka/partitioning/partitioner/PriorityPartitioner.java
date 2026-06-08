package com.scaleatdesign.kafka.partitioning.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Priority-based partitioner that routes high-priority messages
 * to dedicated partitions for faster processing.
 *
 * Key format: "PRIORITY-orderId" (e.g., "HIGH-order123", "LOW-order456")
 *
 * Partition mapping:
 * - HIGH   → Partition 0 (dedicated fast-lane)
 * - MEDIUM → Partitions 1-2 (shared)
 * - LOW    → Partitions 3+ (bulk processing)
 *
 * Use case: SLA-based processing, VIP customer handling.
 */
@Slf4j
public class PriorityPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionsForTopic(topic).size();

        if (key == null || numPartitions <= 1) {
            return 0;
        }

        String keyStr = key.toString().toUpperCase();

        if (keyStr.startsWith("HIGH")) {
            log.debug("HIGH priority message → partition 0");
            return 0;
        } else if (keyStr.startsWith("MEDIUM")) {
            // Distribute across middle partitions
            int partition = 1 + (Math.abs(keyStr.hashCode()) % Math.max(1, numPartitions / 3));
            log.debug("MEDIUM priority message → partition {}", partition);
            return Math.min(partition, numPartitions - 1);
        } else {
            // LOW priority: distribute across remaining partitions
            int lowStart = Math.max(2, numPartitions / 2);
            int partition = lowStart + (Math.abs(keyStr.hashCode()) % (numPartitions - lowStart));
            log.debug("LOW priority message → partition {}", partition);
            return Math.min(partition, numPartitions - 1);
        }
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
