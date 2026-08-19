package com.abhishek.hookrelay.common.repo;

import com.abhishek.hookrelay.common.domain.Delivery;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DeliveryRepository extends JpaRepository<Delivery, UUID> {

    List<Delivery> findByEventIdOrderByCreatedAt(UUID eventId);

    long countByEventId(UUID eventId);
}
