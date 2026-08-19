package com.abhishek.hookrelay.worker.isolation;

import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Caps in-flight requests per endpoint, without ever blocking a worker thread.
 *
 * <p>The acquire is deliberately non-blocking, and that is the whole design. A blocking
 * {@code acquire()} would respect the limit and still hand the thread to the slow endpoint:
 *
 * <pre>
 *   thread 1 ──► endpoint A   holds the permit, 30s request
 *   thread 2 ──► endpoint A   acquire() ......... BLOCKED for 30s
 *   thread 3 ──► endpoint A   acquire() ......... BLOCKED for 30s
 *   thread 4 ──► endpoint A   acquire() ......... BLOCKED for 30s
 * </pre>
 *
 * <p>One request in flight, four threads gone, and every healthy endpoint starved anyway. The scarce
 * resource is the worker thread, not the outbound request — so a failed {@code tryAcquire} returns
 * immediately and the caller puts the delivery back instead of waiting for it.
 *
 * <p>Permits are worker-local. With <em>W</em> workers an endpoint's effective limit is
 * <em>W × max_concurrency</em>; a globally exact limit needs Redis, which BLUEPRINT.md §32 keeps as
 * an optional future improvement.
 */
@Component
public class EndpointSemaphores {

    private record Limited(Semaphore semaphore, int permits) {
    }

    private final ConcurrentHashMap<UUID, Limited> byEndpoint = new ConcurrentHashMap<>();

    /**
     * @return true if a permit was taken. The caller must {@link #release} it in a finally block.
     */
    public boolean tryAcquire(UUID endpointId, int maxConcurrency) {
        return semaphoreFor(endpointId, maxConcurrency).tryAcquire();
    }

    public void release(UUID endpointId) {
        Limited limited = byEndpoint.get(endpointId);
        if (limited != null) {
            limited.semaphore().release();
        }
    }

    public int availablePermits(UUID endpointId, int maxConcurrency) {
        return semaphoreFor(endpointId, maxConcurrency).availablePermits();
    }

    /**
     * Rebuilds the semaphore if the endpoint's configured limit has changed since it was cached, so
     * an operator raising {@code max_concurrency} takes effect without a worker restart.
     */
    private Semaphore semaphoreFor(UUID endpointId, int maxConcurrency) {
        Limited limited = byEndpoint.compute(endpointId, (id, existing) ->
                (existing == null || existing.permits() != maxConcurrency)
                        ? new Limited(new Semaphore(maxConcurrency), maxConcurrency)
                        : existing);
        return limited.semaphore();
    }
}
