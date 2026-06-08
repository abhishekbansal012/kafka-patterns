package com.scaleatdesign.kafka.idempotent.repository;

import com.scaleatdesign.kafka.idempotent.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
