package com.scaleatdesign.kafka.saga.orchestrator;

import com.scaleatdesign.kafka.common.config.KafkaTopics;
import com.scaleatdesign.kafka.saga.entity.SagaState;
import com.scaleatdesign.kafka.saga.model.SagaCommand;
import com.scaleatdesign.kafka.saga.model.SagaReply;
import com.scaleatdesign.kafka.saga.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Saga Orchestrator — coordinates the distributed transaction.
 * Receives replies from participants and decides next step or compensation.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;
    private final SagaStateRepository sagaStateRepository;

    /**
     * Starts a new saga for an order.
     */
    @Transactional
    public String startSaga(String orderId, String customerId, String productId,
                            int quantity, BigDecimal amount) {
        String sagaId = UUID.randomUUID().toString();

        // Persist saga state
        SagaState state = SagaState.builder()
                .sagaId(sagaId)
                .orderId(orderId)
                .currentStep(SagaState.SagaStep.INITIATED)
                .status(SagaState.SagaStatus.IN_PROGRESS)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        sagaStateRepository.save(state);

        // Step 1: Send payment command
        SagaCommand paymentCommand = SagaCommand.builder()
                .sagaId(sagaId)
                .orderId(orderId)
                .customerId(customerId)
                .productId(productId)
                .quantity(quantity)
                .amount(amount)
                .commandType(SagaCommand.CommandType.PROCESS_PAYMENT)
                .correlationId(UUID.randomUUID().toString())
                .build();

        kafkaTemplate.send(KafkaTopics.SAGA_PAYMENTS, sagaId, paymentCommand);
        log.info("Saga started: sagaId={}, orderId={} → Step: PAYMENT", sagaId, orderId);

        return sagaId;
    }

    /**
     * Listens for replies from saga participants.
     */
    @KafkaListener(topics = KafkaTopics.SAGA_ORDERS, groupId = "saga-orchestrator-group")
    @Transactional
    public void handleReply(SagaReply reply) {
        log.info("Saga reply received: sagaId={}, participant={}, status={}",
                reply.getSagaId(), reply.getParticipantName(), reply.getStatus());

        SagaState state = sagaStateRepository.findById(reply.getSagaId())
                .orElseThrow(() -> new IllegalStateException("Unknown saga: " + reply.getSagaId()));

        if (reply.getStatus() == SagaReply.ReplyStatus.SUCCESS) {
            advanceSaga(state, reply);
        } else {
            compensateSaga(state, reply);
        }
    }

    private void advanceSaga(SagaState state, SagaReply reply) {
        switch (reply.getParticipantName()) {
            case "PAYMENT" -> {
                state.setCurrentStep(SagaState.SagaStep.INVENTORY);
                state.setUpdatedAt(Instant.now());
                sagaStateRepository.save(state);

                // Step 2: Reserve inventory
                SagaCommand cmd = buildCommand(state, SagaCommand.CommandType.RESERVE_INVENTORY);
                kafkaTemplate.send(KafkaTopics.SAGA_INVENTORY, state.getSagaId(), cmd);
                log.info("Saga advancing: sagaId={} → Step: INVENTORY", state.getSagaId());
            }
            case "INVENTORY" -> {
                state.setCurrentStep(SagaState.SagaStep.SHIPPING);
                state.setUpdatedAt(Instant.now());
                sagaStateRepository.save(state);

                // Step 3: Schedule shipping
                SagaCommand cmd = buildCommand(state, SagaCommand.CommandType.SCHEDULE_SHIPPING);
                kafkaTemplate.send(KafkaTopics.SAGA_SHIPPING, state.getSagaId(), cmd);
                log.info("Saga advancing: sagaId={} → Step: SHIPPING", state.getSagaId());
            }
            case "SHIPPING" -> {
                state.setCurrentStep(SagaState.SagaStep.COMPLETED);
                state.setStatus(SagaState.SagaStatus.COMPLETED);
                state.setUpdatedAt(Instant.now());
                sagaStateRepository.save(state);
                log.info("✅ Saga COMPLETED: sagaId={}, orderId={}", state.getSagaId(), state.getOrderId());
            }
        }
    }

    private void compensateSaga(SagaState state, SagaReply reply) {
        state.setStatus(SagaState.SagaStatus.COMPENSATING);
        state.setFailureReason(reply.getParticipantName() + ": " + reply.getMessage());
        state.setUpdatedAt(Instant.now());
        sagaStateRepository.save(state);

        log.warn("⚠️ Saga COMPENSATING: sagaId={}, failed at: {}",
                state.getSagaId(), reply.getParticipantName());

        // Compensate in reverse order based on where we failed
        switch (reply.getParticipantName()) {
            case "SHIPPING" -> {
                // Refund payment + release inventory
                kafkaTemplate.send(KafkaTopics.SAGA_INVENTORY,
                        state.getSagaId(), buildCommand(state, SagaCommand.CommandType.RELEASE_INVENTORY));
                kafkaTemplate.send(KafkaTopics.SAGA_PAYMENTS,
                        state.getSagaId(), buildCommand(state, SagaCommand.CommandType.REFUND_PAYMENT));
            }
            case "INVENTORY" -> {
                // Refund payment only
                kafkaTemplate.send(KafkaTopics.SAGA_PAYMENTS,
                        state.getSagaId(), buildCommand(state, SagaCommand.CommandType.REFUND_PAYMENT));
            }
            case "PAYMENT" -> {
                // Nothing to compensate
                state.setStatus(SagaState.SagaStatus.COMPENSATED);
                sagaStateRepository.save(state);
            }
        }
    }

    private SagaCommand buildCommand(SagaState state, SagaCommand.CommandType type) {
        return SagaCommand.builder()
                .sagaId(state.getSagaId())
                .orderId(state.getOrderId())
                .commandType(type)
                .correlationId(UUID.randomUUID().toString())
                .build();
    }
}
