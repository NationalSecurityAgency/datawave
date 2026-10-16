package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;

/**
 * The implementation of {@link QueryHeartbeatCache}.
 */
public class QueryHeartbeatCache {

    private static final Logger log = LoggerFactory.getLogger(QueryHeartbeatCache.class);

    private final Cache<String,QueryHeartbeat> cache = Caffeine.newBuilder().removalListener((key, value, cause) -> {
        if (cause.wasEvicted() && log.isTraceEnabled()) {
            log.trace("Evicted heartbeat for query {}", key);
        }
    }).build();

    /**
     * An executor that will periodically clean up the internal cache of any stopped heartbeats.
     */
    private final ScheduledExecutorService executor;

    public QueryHeartbeatCache(long cleanupInterval, TimeUnit cleanupUnit) {
        Preconditions.checkArgument(cleanupInterval > 0, "cleanup interval must be greater than 0");
        Preconditions.checkNotNull(cleanupUnit, "cleanup interval unit must not be null");

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Initializing with periodic cleanup of stopped heartbeats every {} {}", cleanupInterval, cleanupUnit);
            }
            executor.scheduleAtFixedRate(this::removeAllStoppedHeartbeats, cleanupInterval, cleanupInterval, cleanupUnit);
            this.executor = executor;
        } catch (Exception e) {
            log.error("Failed to initialize QueryHeartbeatCache", e);
            // If an error occurs, ensure the executor is closed.
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * Iterates through the entries of the cache and evicts all {@link QueryHeartbeat} instances that are stopped. This will be called on regular basis with the
     * cleanup interval that this {@link QueryHeartbeatCache} was configured with.
     *
     * @throws IllegalStateException
     *             if this {@link QueryHeartbeatCache} has been closed
     */
    private void removeAllStoppedHeartbeats() {
        log.trace("Removing all stopped heartbeats");
        Set<String> queryIds = new HashSet<>();
        // Collect the query IDs of all heartbeats that are considered stopped.
        for (QueryHeartbeat heartbeat : cache.asMap().values()) {
            if (heartbeat.isStopped()) {
                queryIds.add(heartbeat.getQueryId());
            }
        }
        // Invalidate them.
        this.cache.invalidateAll(queryIds);
        if (log.isTraceEnabled()) {
            log.trace("Removed stopped heartbeats {}", queryIds);
        }
    }

    /**
     * Associate the given {@link QueryHeartbeat} with its query ID in the cache. A listener will be set in the heartbeat that will notify this cache when
     * {@link QueryHeartbeat#stop()} is called and automatically evict the heartbeat from the cache.
     *
     * @param heartbeat
     *            the heartbeat
     * @throws NullPointerException
     *             if heartbeat is null
     * @throws IllegalStateException
     *             if this {@link QueryHeartbeatCache} has been closed
     */
    public void put(QueryHeartbeat heartbeat) {
        Preconditions.checkNotNull(heartbeat, "heartbeat must not be null");
        if (log.isTraceEnabled()) {
            log.trace("Adding heartbeat {}", heartbeat);
        }
        // Add a listener to the heartbeat that will automatically trigger the heartbeat's eviction if it is ever stopped outside the cache's stop and
        // remove method.
        heartbeat.addListener(new HeartbeatStoppedListener(this));
        cache.put(heartbeat.getQueryId(), heartbeat);

    }

    /**
     * Return the {@link QueryHeartbeat} associated with the given query ID, or null if there is no cached {@link QueryHeartbeat} with the query ID or this
     * {@link QueryHeartbeatCache} has been closed.
     *
     * @param queryId
     *            the query ID
     * @return the heartbeat, possibly null
     */
    public QueryHeartbeat get(String queryId) {
        return cache.getIfPresent(queryId);
    }

    /**
     * Return the set of query ID keys in the cache, or an empty set if this {@link QueryHeartbeatCache} has been closed.
     *
     * @return the query IDs
     */
    public Set<String> getQueryIds() {
        return cache.asMap().keySet();
    }

    /**
     * If any {@link QueryHeartbeat} values are stored in the cache for the given query IDs, remove them from the cache and stop them via
     * {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     *
     * @param queryIds
     *            the query IDs
     * @throws NullPointerException
     *             if queryIds is null
     * @throws IllegalStateException
     *             if this {@link QueryHeartbeatCache} has been closed
     */
    public void stopAndRemove(Collection<String> queryIds) {
        if (log.isTraceEnabled()) {
            log.trace("Stopping heartbeats for {} queries", queryIds.size());
        }
        if (!queryIds.isEmpty()) {
            ConcurrentMap<String,QueryHeartbeat> map = this.cache.asMap();
            for (String queryId : queryIds) {
                if (queryId != null) {
                    QueryHeartbeat heartbeat = map.remove(queryId);
                    stopHeartbeat(heartbeat);
                }
            }
        }
    }

    /**
     * If a {@link QueryHeartbeat} is stored in the cache for the given query ID, remove it from the cache and stop the heartbeat via
     * {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     *
     * @param queryId
     *            the query ID
     * @throws NullPointerException
     *             if queryId is null
     * @throws IllegalStateException
     *             if this {@link QueryHeartbeatCache} has been closed
     */
    public void stopAndRemove(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Stopping and removing heartbeat for query {}", queryId);
        }
        QueryHeartbeat heartbeat = this.cache.asMap().remove(queryId);
        stopHeartbeat(heartbeat);
    }

    /**
     * Attempt to stop the given heartbeat via {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     *
     * @param heartbeat
     *            the heartbeat to stop
     */
    private void stopHeartbeat(QueryHeartbeat heartbeat) {
        if (heartbeat != null) {
            try {
                heartbeat.stopWithoutNotifyingListener();
            } catch (Exception e) {
                log.error("Error stopping heartbeat for query {}", heartbeat.getQueryId(), e);
            }
        }
    }

    /**
     * Closes this {@link QueryHeartbeatCache} and cleans up its internal resources.
     *
     * @param stopAllHeartbeats
     *            whether all heartbeats currently in the cache should be stopped before the cache is cleared
     */
    public void close(boolean stopAllHeartbeats) {
        if (log.isDebugEnabled()) {
            log.debug("Closing. Heartbeats will {}be stopped.", (stopAllHeartbeats ? "" : "not "));
        }
        // Shut down the executor service.
        if (this.executor != null) {
            try {
                this.executor.shutdown();
                boolean shutdown = this.executor.awaitTermination(1, TimeUnit.MINUTES);
                if (!shutdown) {
                    log.warn("Executor did not terminate within 1 minute");
                }
            } catch (Exception e) {
                log.error("Error shutting down executor", e);
            }
        }

        // If heartbeats should be stopped, remove and stop each heartbeat in the cache.
        if (stopAllHeartbeats) {
            for (String queryId : Set.copyOf(this.cache.asMap().keySet())) {
                try {
                    QueryHeartbeat heartbeat = this.cache.asMap().remove(queryId);
                    stopHeartbeat(heartbeat);
                } catch (Exception e) {
                    log.error("Error stopping heartbeat for query {}", queryId, e);
                }
            }
        } else {
            // Otherwise simply clear the cache.
            this.cache.invalidateAll();
        }
    }

    /**
     * A simple listener that can be used to listen for when to evict a {@link QueryHeartbeat} from the cache.
     */
    private static class HeartbeatStoppedListener implements Consumer<String> {

        private final QueryHeartbeatCache cache;

        public HeartbeatStoppedListener(QueryHeartbeatCache cache) {
            this.cache = cache;
        }

        @Override
        public void accept(String queryId) {
            this.cache.stopAndRemove(queryId);
        }
    }
}
