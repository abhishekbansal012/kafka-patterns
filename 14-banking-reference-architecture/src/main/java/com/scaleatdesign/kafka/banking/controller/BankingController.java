package com.scaleatdesign.kafka.banking.controller;

import com.scaleatdesign.kafka.banking.account.Account;
import com.scaleatdesign.kafka.banking.account.AccountRepository;
import com.scaleatdesign.kafka.banking.account.AccountService;
import com.scaleatdesign.kafka.banking.transfer.Transfer;
import com.scaleatdesign.kafka.banking.transfer.TransferService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/banking")
@RequiredArgsConstructor
public class BankingController {

    private final AccountService accountService;
    private final TransferService transferService;
    private final AccountRepository accountRepository;

    // === Account Operations ===

    @PostMapping("/accounts")
    public ResponseEntity<Account> createAccount(@RequestBody Map<String, Object> request) {
        BigDecimal deposit = request.get("initialDeposit") != null ?
                new BigDecimal(request.get("initialDeposit").toString()) : BigDecimal.ZERO;
        Account account = accountService.createAccount((String) request.get("ownerName"), deposit);
        return ResponseEntity.ok(account);
    }

    @GetMapping("/accounts/{accountId}")
    public ResponseEntity<Account> getAccount(@PathVariable String accountId) {
        return accountRepository.findById(accountId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // === Transfer Operations ===

    @PostMapping("/transfers")
    public ResponseEntity<Transfer> initiateTransfer(@RequestBody TransferRequest request) {
        Transfer transfer = transferService.initiateTransfer(
                request.fromAccountId(), request.toAccountId(), request.amount());
        return ResponseEntity.accepted().body(transfer);
    }

    public record TransferRequest(String fromAccountId, String toAccountId, BigDecimal amount) {}
}
