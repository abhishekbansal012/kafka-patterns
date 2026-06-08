package com.scaleatdesign.kafka.eventsourcing.aggregate;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Account aggregate — reconstructed from events.
 * No direct state mutation — all changes through events.
 */
@Data
@Slf4j
public class AccountAggregate {

    private String accountId;
    private String ownerName;
    private BigDecimal balance;
    private String status; // ACTIVE, CLOSED
    private int version;

    public AccountAggregate() {
        this.balance = BigDecimal.ZERO;
        this.status = "INACTIVE";
        this.version = 0;
    }

    /**
     * Apply an event to reconstruct state.
     * This is the ONLY way to change aggregate state.
     */
    public void apply(String eventType, Map<String, Object> payload) {
        switch (eventType) {
            case "ACCOUNT_CREATED" -> {
                this.accountId = (String) payload.get("accountId");
                this.ownerName = (String) payload.get("ownerName");
                this.balance = BigDecimal.ZERO;
                this.status = "ACTIVE";
            }
            case "MONEY_DEPOSITED" -> {
                BigDecimal amount = new BigDecimal(payload.get("amount").toString());
                this.balance = this.balance.add(amount);
            }
            case "MONEY_WITHDRAWN" -> {
                BigDecimal amount = new BigDecimal(payload.get("amount").toString());
                this.balance = this.balance.subtract(amount);
            }
            case "ACCOUNT_CLOSED" -> {
                this.status = "CLOSED";
            }
            default -> log.warn("Unknown event type: {}", eventType);
        }
        this.version++;
    }
}
