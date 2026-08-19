package com.abhishek.hookrelay.api.web;

import com.abhishek.hookrelay.api.error.ApiException;
import com.abhishek.hookrelay.api.service.IngestResult;
import com.abhishek.hookrelay.api.service.IngestService;
import com.abhishek.hookrelay.api.web.dto.EventResponse;
import com.abhishek.hookrelay.api.web.dto.IngestEventRequest;
import com.abhishek.hookrelay.api.web.dto.IngestEventResponse;
import com.abhishek.hookrelay.common.domain.Event;
import com.abhishek.hookrelay.common.repo.DeliveryRepository;
import com.abhishek.hookrelay.common.repo.EventRepository;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/events")
public class EventController {

    private final IngestService ingest;
    private final EventRepository events;
    private final DeliveryRepository deliveries;

    public EventController(IngestService ingest, EventRepository events, DeliveryRepository deliveries) {
        this.ingest = ingest;
        this.events = events;
        this.deliveries = deliveries;
    }

    /**
     * Accepts an event.
     *
     * <p>202 means this call created the event; 200 means the same {@code Idempotency-Key} had
     * already been used with an identical request and the original event is being returned. The
     * status code alone tells the producer which happened.
     *
     * <p>Either way the response is sent only after the transaction has committed, so the promise
     * the status code makes — "this work is durably recorded" — is true at the moment it is made.
     */
    @PostMapping
    public ResponseEntity<IngestEventResponse> ingest(
            @RequestHeader("X-Tenant-Id") UUID tenantId,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestBody IngestEventRequest request) {

        IngestResult result = ingest.ingest(
                tenantId, idempotencyKey, request.eventType(), request.payload());

        HttpStatus status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status)
                .body(new IngestEventResponse(result.eventId(), result.deliveriesCreated()));
    }

    @GetMapping("/{id}")
    public EventResponse get(@RequestHeader("X-Tenant-Id") UUID tenantId,
                             @PathVariable UUID id) {
        Event event = events.findByIdAndTenantId(id, tenantId)
                .orElseThrow(() -> ApiException.notFound("event_not_found",
                        "no event " + id + " for this tenant"));
        return EventResponse.from(event, deliveries.findByEventIdOrderByCreatedAt(id));
    }
}
