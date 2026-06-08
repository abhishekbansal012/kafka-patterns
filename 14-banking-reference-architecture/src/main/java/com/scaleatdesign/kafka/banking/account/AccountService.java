package com.scaleatdesign.kafka.banking.account;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public Account createAccount(String ownerName, BigDecimal initialDeposit) {
        Account account = Account.builder()
                .accountId(UUID.randomUUID().toString())
                .ownerName(ownerName)
                .balance(initialDeposit != null ? initialDeposit : BigDecimal.ZERO)
                .status("ACTIVE")
                .createdAt(Instant.now())
                .build();

        accountRepository.save(account);

        kafkaTemplate.send(KafkaTopics.BANK_ACCOUNTS, account.getAccountId(), Map.of(
                "eventType", "ACCOUNT_CREATED",
                "accountId", account.getAccountId(),
                "ownerName", ownerName,
                "balance", account.getBalance(),
                "timestamp", Instant.now().toString()
        ));

        log.info("Account created: {}", account.getAccountId());
        return account;
    }

    @Transactional
    public void debit(String accountId, BigDecimal amount, String transferId) {
        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found: " + accountId));

        if (account.getBalance().compareTo(amount) < 0) {
            throw new IllegalStateException("Insufficient balance for account: " + accountId);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        kafkaTemplate.send(KafkaTopics.BANK_ACCOUNTS, accountId, Map.of(
                "eventType", "ACCOUNT_DEBITED",
                "accountId", accountId,
                "amount", amount,
                "transferId", transferId,
                "newBalance", account.getBalance(),
                "timestamp", Instant.now().toString()
        ));
        log.info("Account debited: {} (amount: {}, transfer: {})", accountId, amount, transferId);
    }

    @Transactional
    public void credit(String accountId, BigDecimal amount, String transferId) {
        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new IllegalArgumentException("Account not found: " + accountId));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        kafkaTemplate.send(KafkaTopics.BANK_ACCOUNTS, accountId, Map.of(
                "eventType", "ACCOUNT_CREDITED",
                "accountId", accountId,
                "amount", amount,
                "transferId", transferId,
                "newBalance", account.getBalance(),
                "timestamp", Instant.now().toString()
        ));
        log.info("Account credited: {} (amount: {}, transfer: {})", accountId, amount, transferId);
    }
}
