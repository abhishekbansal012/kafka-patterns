package com.scaleatdesign.kafka.saga.repository;

import com.scaleatdesign.kafka.saga.entity.SagaState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface SagaStateRepository extends JpaRepository<SagaState, String> {
}
