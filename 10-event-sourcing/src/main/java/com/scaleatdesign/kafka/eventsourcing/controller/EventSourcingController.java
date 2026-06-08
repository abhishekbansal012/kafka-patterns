package com.scaleatdesign.kafka.eventsourcing.controller;

import com.scaleatdesign.kafka.eventsourcing.aggregate.AccountAggregate;
import com.scaleatdesign.kafka.eventsourcing.service.EventStoreService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/event-sourcing/accounts")
@RequiredArgsConstructor
public class EventSourcingController {

    private final EventStoreService eventStoreService;

    @PostMapping
    public ResponseEntity<Map<String, String>> createAccount(@RequestBody Map<String, String> request) {
        String accountId = UUID.randomUUID().toString();
        eventStoreService.appendEvent(accountId, "ACCOUNT", "ACCOUNT_CREATED",
                Map.of("accountId", accountId, "ownerName", request.get("ownerName")));
        return ResponseEntity.ok(Map.of("accountId", accountId, "status", "CREATED"));
    }

    @PostMapping("/{accountId}/deposit")
    public ResponseEntity<Map<String, Object>> deposit(@PathVariable String accountId, @RequestBody Map<String, Object> request) {
        eventStoreService.appendEvent(accountId, "ACCOUNT", "MONEY_DEPOSITED",
                Map.of("accountId", accountId, "amount", request.get("amount")));
        AccountAggregate agg = eventStoreService.loadAggregate(accountId);
        return ResponseEntity.ok(Map.of("accountId", accountId, "newBalance", agg.getBalance()));
    }

    @PostMapping("/{accountId}/withdraw")
    public ResponseEntity<Map<String, Object>> withdraw(@PathVariable String accountId, @RequestBody Map<String, Object> request) {
        eventStoreService.appendEvent(accountId, "ACCOUNT", "MONEY_WITHDRAWN",
                Map.of("accountId", accountId, "amount", request.get("amount")));
        AccountAggregate agg = eventStoreService.loadAggregate(accountId);
        return ResponseEntity.ok(Map.of("accountId", accountId, "newBalance", agg.getBalance()));
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<AccountAggregate> getAccount(@PathVariable String accountId) {
        AccountAggregate aggregate = eventStoreService.loadAggregate(accountId);
        return ResponseEntity.ok(aggregate);
    }
}
