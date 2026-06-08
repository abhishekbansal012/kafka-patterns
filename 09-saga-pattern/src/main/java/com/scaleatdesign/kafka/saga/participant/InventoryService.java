package com.scaleatdesign.kafka.saga.participant;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.saga.model.SagaCommand;
import com.scaleatdesign.kafka.saga.model.SagaReply;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Inventory saga participant — handles stock reservation.
 * Simulates failure when quantity > 10 (out of stock).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaTopics.SAGA_INVENTORY, groupId = "saga-inventory-group")
    public void handleCommand(SagaCommand command) {
        log.info("Inventory service received: sagaId={}, type={}", command.getSagaId(), command.getCommandType());

        SagaReply reply;
        switch (command.getCommandType()) {
            case RESERVE_INVENTORY -> {
                boolean success = command.getQuantity() <= 10;
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("INVENTORY")
                        .status(success ? SagaReply.ReplyStatus.SUCCESS : SagaReply.ReplyStatus.FAILURE)
                        .message(success ? "Inventory reserved" : "Out of stock")
                        .correlationId(command.getCorrelationId())
                        .build();
                log.info("Inventory {}: sagaId={}", success ? "RESERVED" : "OUT OF STOCK", command.getSagaId());
            }
            case RELEASE_INVENTORY -> {
                log.info("📦 INVENTORY RELEASED for sagaId={}", command.getSagaId());
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("INVENTORY")
                        .status(SagaReply.ReplyStatus.SUCCESS)
                        .message("Inventory released")
                        .correlationId(command.getCorrelationId())
                        .build();
            }
            default -> {
                return;
            }
        }

        replyTemplate.send(KafkaTopics.SAGA_ORDERS, command.getSagaId(), reply);
    }
}
