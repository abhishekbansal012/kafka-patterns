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
 * Shipping saga participant — schedules delivery.
 * Always succeeds in this demo.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingService {

    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaTopics.SAGA_SHIPPING, groupId = "saga-shipping-group")
    public void handleCommand(SagaCommand command) {
        log.info("Shipping service received: sagaId={}, type={}", command.getSagaId(), command.getCommandType());

        SagaReply reply;
        switch (command.getCommandType()) {
            case SCHEDULE_SHIPPING -> {
                log.info("🚚 Shipping scheduled for sagaId={}", command.getSagaId());
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("SHIPPING")
                        .status(SagaReply.ReplyStatus.SUCCESS)
                        .message("Shipping scheduled")
                        .correlationId(command.getCorrelationId())
                        .build();
            }
            case CANCEL_SHIPPING -> {
                log.info("❌ Shipping CANCELLED for sagaId={}", command.getSagaId());
                reply = SagaReply.builder()
                        .sagaId(command.getSagaId())
                        .orderId(command.getOrderId())
                        .participantName("SHIPPING")
                        .status(SagaReply.ReplyStatus.SUCCESS)
                        .message("Shipping cancelled")
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
