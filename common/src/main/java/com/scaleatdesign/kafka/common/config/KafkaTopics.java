package com.scaleatdesign.kafka.common.config;

/**
 * Central registry of all Kafka topic names used across modules.
 */
public final class KafkaTopics {

    private KafkaTopics() {}

    // Module 01 - Producer Consumer
    public static final String ORDERS = "orders";
    public static final String ORDER_CONFIRMATIONS = "order-confirmations";

    // Module 02 - Avro Schema Registry
    public static final String ORDERS_AVRO = "orders-avro";

    // Module 03 - Partitioning
    public static final String ORDERS_PARTITIONED = "orders-partitioned";

    // Module 04 - Consumer Groups
    public static final String ORDERS_GROUPED = "orders-grouped";

    // Module 05 - Retry
    public static final String ORDERS_RETRY = "orders-retry";

    // Module 06 - Dead Letter
    public static final String ORDERS_DLT = "orders-dlt";
    public static final String ORDERS_DLT_DEAD = "orders-dlt.DLT";

    // Module 07 - Idempotent
    public static final String ORDERS_IDEMPOTENT = "orders-idempotent";

    // Module 08 - Transactional Outbox
    public static final String OUTBOX_EVENTS = "outbox-events";

    // Module 09 - Saga
    public static final String SAGA_ORDERS = "saga-orders";
    public static final String SAGA_PAYMENTS = "saga-payments";
    public static final String SAGA_INVENTORY = "saga-inventory";
    public static final String SAGA_SHIPPING = "saga-shipping";

    // Module 10 - Event Sourcing
    public static final String EVENT_STORE = "event-store";

    // Module 11 - CQRS
    public static final String CQRS_COMMANDS = "cqrs-commands";
    public static final String CQRS_EVENTS = "cqrs-events";

    // Module 12 - Exactly Once
    public static final String EXACTLY_ONCE_INPUT = "exactly-once-input";
    public static final String EXACTLY_ONCE_OUTPUT = "exactly-once-output";

    // Module 13 - Payment Processing
    public static final String PAYMENTS = "payments";
    public static final String PAYMENT_RESULTS = "payment-results";
    public static final String PAYMENT_NOTIFICATIONS = "payment-notifications";

    // Module 14 - Banking
    public static final String BANK_ACCOUNTS = "bank-accounts";
    public static final String BANK_TRANSFERS = "bank-transfers";
    public static final String BANK_FRAUD = "bank-fraud";
    public static final String BANK_NOTIFICATIONS = "bank-notifications";
}
