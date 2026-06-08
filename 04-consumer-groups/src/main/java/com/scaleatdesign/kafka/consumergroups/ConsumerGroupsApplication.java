package com.scaleatdesign.kafka.consumergroups;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Module 04: Consumer Groups & Rebalancing
 *
 * Demonstrates:
 * - Multiple consumers sharing partitions within a group
 * - ConsumerRebalanceListener for rebalance events
 * - Manual vs automatic offset commit
 * - Concurrent consumer configuration
 * - Consumer lag and group coordination
 *
 * Run multiple instances to see rebalancing in action.
 */
@SpringBootApplication
public class ConsumerGroupsApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerGroupsApplication.class, args);
    }
}
