package com.scaleatdesign.kafka.cqrs.query;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductReadModelRepository extends JpaRepository<ProductReadModel, String> {

    List<ProductReadModel> findByCategory(String category);

    List<ProductReadModel> findByInStockTrue();
}
