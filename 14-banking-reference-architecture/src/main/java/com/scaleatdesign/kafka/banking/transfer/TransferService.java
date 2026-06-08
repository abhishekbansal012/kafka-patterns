package com.scaleatdesign.kafka.banking.transfer;

import com.scaleatdesign.kafka.banking.account.AccountService;
import com.scaleatdesign.kafka.banking.fraud.FraudDetectionService;
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

/**
 * Transfer orchestrator — saga-style fund transfer between accounts.
 *
 * Steps:
 * 1. Fraud check
 * 2. Debit source account
 * 3. Credit destination account
 * 4. Notify
 *
 * Compensation on failure:
 * - If credit fails after debit → refund source account
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

    private final AccountService accountService;
    private final FraudDetectionService fraudService;
    private final TransferRepository transferRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public Transfer initiateTransfer(String fromAccountId, String toAccountId, BigDecimal amount) {
        String transferId = UUID.randomUUID().toString();

        Transfer transfer = Transfer.builder()
                .transferId(transferId)
                .fromAccountId(fromAccountId)
                .toAccountId(toAccountId)
                .amount(amount)
                .status(Transfer.TransferStatus.PENDING)
                .createdAt(Instant.now())
                .build();
        transferRepository.save(transfer);

        try {
            // Step 1: Fraud check
            fraudService.checkFraud(transferId, fromAccountId, toAccountId, amount);
            transfer.setStatus(Transfer.TransferStatus.FRAUD_CHECKED);
            transferRepository.save(transfer);

            // Step 2: Debit source
            accountService.debit(fromAccountId, amount, transferId);
            transfer.setStatus(Transfer.TransferStatus.DEBITED);
            transferRepository.save(transfer);

            // Step 3: Credit destination
            accountService.credit(toAccountId, amount, transferId);
            transfer.setStatus(Transfer.TransferStatus.COMPLETED);
            transfer.setCompletedAt(Instant.now());
            transferRepository.save(transfer);

            // Step 4: Notify
            kafkaTemplate.send(KafkaTopics.BANK_NOTIFICATIONS, transferId, Map.of(
                    "type", "TRANSFER_COMPLETED",
                    "transferId", transferId,
                    "from", fromAccountId,
                    "to", toAccountId,
                    "amount", amount,
                    "timestamp", Instant.now().toString()
            ));

            log.info("✅ Transfer completed: {} → {} (amount: {})", fromAccountId, toAccountId, amount);
        } catch (Exception e) {
            log.error("Transfer failed: {}", transferId, e);
            transfer.setStatus(Transfer.TransferStatus.FAILED);
            transfer.setFailureReason(e.getMessage());
            transferRepository.save(transfer);

            // Compensate if we already debited
            if (transfer.getStatus() == Transfer.TransferStatus.DEBITED) {
                accountService.credit(fromAccountId, amount, transferId + "-refund");
                log.warn("Compensation: Refunded source account {}", fromAccountId);
            }

            // Publish to DLT
            kafkaTemplate.send(KafkaTopics.BANK_TRANSFERS + ".DLT", transferId, Map.of(
                    "transferId", transferId,
                    "error", e.getMessage(),
                    "timestamp", Instant.now().toString()
            ));
        }

        return transfer;
    }
}
