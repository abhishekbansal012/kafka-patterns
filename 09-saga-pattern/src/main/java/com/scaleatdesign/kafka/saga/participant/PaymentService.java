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
 * Payment saga participant — processes payment commands.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaTopics.SAGA_PAYMENTS, groupId = "saga-payment-group")
    public void handleCommand(SagaCommand command) {
        log.info("Payment service received: sagaId={}, type={}", command.getSagaId(), command.getCommandType());

        SagaReply reply;
        switch (command.getCommandType()) {
            case PROCESS_PAYMENT -> {
                // Simulate payment processing (fail if amount > 1000)
                boolean success = command.getAmount() == null || command.getAmount().doubleValue() <= 1000;
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("PAYMENT")
                        .status(success ? SagaReply.ReplyStatus.SUCCESS : SagaReply.ReplyStatus.FAILURE)
                        .message(success ? "Payment processed" : "Insufficient funds")
                        .correlationId(command.getCorrelationId())
                        .build();
                log.info("Payment {}: sagaId={}", success ? "SUCCESS" : "FAILED", command.getSagaId());
            }
            case REFUND_PAYMENT -> {
                log.info("💰 REFUND processed for sagaId={}", command.getSagaId());
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("PAYMENT")
                        .status(SagaReply.ReplyStatus.SUCCESS)
                        .message("Payment refunded")
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
