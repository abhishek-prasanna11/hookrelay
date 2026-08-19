package com.abhishek.hookrelay.api.web;

import com.abhishek.hookrelay.api.error.ApiException;
import com.abhishek.hookrelay.api.web.dto.DeliveryResponse;
import com.abhishek.hookrelay.common.domain.Delivery;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.DeliveryAttemptRepository;
import com.abhishek.hookrelay.common.repo.DeliveryRepository;
import com.abhishek.hookrelay.common.repo.EventRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/deliveries")
public class DeliveryController {

    private final DeliveryRepository deliveries;
    private final DeliveryAttemptRepository attempts;
    private final EventRepository events;

    public DeliveryController(DeliveryRepository deliveries,
                              DeliveryAttemptRepository attempts,
                              EventRepository events) {
        this.deliveries = deliveries;
        this.attempts = attempts;
        this.events = events;
    }

    /**
     * Delivery status plus its full attempt history.
     *
     * <p>Deliveries carry no tenant column of their own, so the tenant check goes through the
     * parent event. Skipping it would let any caller read any tenant's delivery history — including
     * response bodies from their customers' endpoints — by guessing a UUID.
     */
    @GetMapping("/{id}")
    public DeliveryResponse get(@RequestHeader("X-Tenant-Id") UUID tenantId,
                                @PathVariable UUID id) {
        Delivery delivery = deliveries.findById(id)
                .orElseThrow(() -> ApiException.notFound("delivery_not_found", "no delivery " + id));

        Event event = events.findById(delivery.getEventId()).orElseThrow(
                () -> new IllegalStateException("delivery " + id + " references missing event"));
        if (!event.getTenantId().equals(tenantId)) {
            // Deliberately the same response as a genuinely missing delivery, so this endpoint
            // cannot be used to probe which delivery ids exist.
            throw ApiException.notFound("delivery_not_found", "no delivery " + id);
        }

        return DeliveryResponse.from(delivery, attempts.findByDeliveryIdOrderByAttemptNo(id));
    }
}
