package com.scaleatdesign.kafka.eventsourcing.repository;

import com.scaleatdesign.kafka.eventsourcing.entity.EventStoreEntry;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EventStoreRepository extends JpaRepository<EventStoreEntry, Long> {

    List<EventStoreEntry> findByAggregateIdOrderByVersionAsc(String aggregateId);

    @Query("SELECT MAX(e.version) FROM EventStoreEntry e WHERE e.aggregateId = :aggregateId")
    Integer findMaxVersionByAggregateId(String aggregateId);
}
