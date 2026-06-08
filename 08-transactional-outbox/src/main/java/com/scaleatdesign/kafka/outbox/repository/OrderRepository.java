package com.scaleatdesign.kafka.outbox.repository;

import com.scaleatdesign.kafka.outbox.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, String> {
}
