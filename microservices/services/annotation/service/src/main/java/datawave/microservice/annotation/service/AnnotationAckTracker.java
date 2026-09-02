package datawave.microservice.annotation.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.springframework.stereotype.Component;

/**
 * Tracks in-flight annotation write operations by correlation ID so that async ack/timeout callbacks can resolve the original request.
 */
@Component
public class AnnotationAckTracker {

    private final Map<String,CountDownLatch> correlationLatchMap = new ConcurrentHashMap<>();

    /**
     * Register a new latch for the given correlation ID. If a latch already exists for this ID, the existing latch is returned instead.
     *
     * @param correlationId
     *            the correlation ID to register
     * @param latch
     *            the latch to register
     * @return the previously-registered latch for this ID, or {@code null} if none existed
     */
    public CountDownLatch putIfAbsent(String correlationId, CountDownLatch latch) {
        return correlationLatchMap.putIfAbsent(correlationId, latch);
    }

    /**
     * Remove and return the latch for the given correlation ID.
     *
     * @param correlationId
     *            the correlation ID whose latch should be removed
     * @param latch
     *            the expected latch instance
     * @return {@code true} if the latch was removed, {@code false} otherwise
     */
    public boolean remove(String correlationId, CountDownLatch latch) {
        return correlationLatchMap.remove(correlationId, latch);
    }

    /**
     * Count down the latch associated with the given correlation ID, if one exists.
     *
     * @param correlationId
     *            the correlation ID whose latch should be counted down
     * @return {@code true} if a latch was found and counted down, {@code false} otherwise
     */
    public boolean countdown(String correlationId) {
        if (correlationLatchMap.containsKey(correlationId)) {
            CountDownLatch latch = correlationLatchMap.get(correlationId);
            if (latch != null) {
                latch.countDown();
                return true;
            }
        }
        return false;
    }

    /**
     * Check whether a latch exists for the given correlation ID.
     *
     * @param correlationId
     *            the correlation ID to check
     * @return {@code true} if a latch exists, {@code false} otherwise
     */
    public boolean contains(String correlationId) {
        return correlationLatchMap.containsKey(correlationId);
    }

    /**
     * Check whether there are no tracked latches.
     *
     * @return {@code true} if no latches are currently tracked
     */
    public boolean isEmpty() {
        return correlationLatchMap.isEmpty();
    }

    /**
     * Clear all tracked latches. Used between tests to prevent leakage.
     */
    public void clear() {
        correlationLatchMap.clear();
    }
}
