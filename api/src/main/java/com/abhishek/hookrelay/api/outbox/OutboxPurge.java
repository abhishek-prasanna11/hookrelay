package com.abhishek.hookrelay.api.outbox;

import com.abhishek.hookrelay.common.repo.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Deletes outbox rows whose message the broker confirmed long enough ago to be useless for
 * diagnosis.
 *
 * <p>This ships in the same phase as the thing that fills the table, on purpose. An outbox that is
 * only ever inserted into and updated is a slow-motion outage: it works perfectly for months and
 * then the table is large enough to matter, usually at the worst time. Retaining a window rather
 * than deleting on publish keeps {@code published_at} available for debugging a recent incident.
 */
@Component
public class OutboxPurge {

    private static final Logger log = LoggerFactory.getLogger(OutboxPurge.class);

    private final OutboxEventRepository outbox;
    private final int retentionHours;
    private final boolean enabled;

    public OutboxPurge(OutboxEventRepository outbox,
                       @Value("${hookrelay.outbox.purge.retention-hours}") int retentionHours,
                       @Value("${hookrelay.outbox.purge.enabled}") boolean enabled) {
        this.outbox = outbox;
        this.retentionHours = retentionHours;
        this.enabled = enabled;
    }

    /**
     * The schedule is gated by a flag checked here rather than by {@code @ConditionalOnProperty} on
     * the class, so that disabling the timer does not also remove the bean. Tests need to invoke
     * {@link #purgeNow} deterministically without a background timer racing them.
     */
    @Scheduled(fixedDelayString = "${hookrelay.outbox.purge.interval-ms}",
            initialDelayString = "${hookrelay.outbox.purge.interval-ms}")
    public void scheduledPurge() {
        if (!enabled) {
            return;
        }
        OffsetDateTime cutoff = OffsetDateTime.now(ZoneOffset.UTC).minusHours(retentionHours);
        int deleted = purgeNow(cutoff);
        if (deleted > 0) {
            log.info("purged {} outbox row(s) published before {}", deleted, cutoff);
        }
    }

    /** Deletes every row published before {@code cutoff}. */
    @Transactional
    public int purgeNow(OffsetDateTime cutoff) {
        return outbox.purgePublishedBefore(cutoff);
    }
}
