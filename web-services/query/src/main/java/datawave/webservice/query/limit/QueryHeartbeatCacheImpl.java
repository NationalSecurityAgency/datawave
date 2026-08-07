package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Singleton;
import javax.ejb.Startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * The implementation of {@link QueryHeartbeatCache}.
 */
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Singleton
@Startup
public class QueryHeartbeatCacheImpl implements QueryHeartbeatCache {

    private static final Logger log = LoggerFactory.getLogger(QueryHeartbeatCacheImpl.class);

    private final Cache<String,QueryHeartbeat> cache = Caffeine.newBuilder().removalListener((key, value, cause) -> {
        if (cause.wasEvicted() && log.isTraceEnabled()) {
            log.trace("Evicted heartbeat for query {}", key);
        }
    }).build();

    private long cleanupInterval = 10;
    private TimeUnit cleanupUnit = TimeUnit.MINUTES;
    private ScheduledExecutorService scheduler;

    /**
     * Set the interval for which {@link QueryHeartbeatCacheImpl#removeAllStoppedHeartbeats()} should be called.
     *
     * @param cleanupInterval
     *            the interval
     */
    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Set the time unit of the cleanup interval.
     *
     * @param cleanupUnit
     *            the cleanup interval time unit
     */
    public void setCleanupUnit(TimeUnit cleanupUnit) {
        this.cleanupUnit = cleanupUnit;
    }

    /**
     * Set up this heartbeat cache.
     */
    @PostConstruct
    public void setup() {
        log.debug("Initializing with cleanup schedule that will run every {} {}", cleanupInterval, cleanupUnit);
        // If the PersistentNodes within a QueryHeartbeat are ever stopped due to a connection failure, and not via to QueryHeartbeat.stop(), the heartbeat will
        // not automatically evict itself from the cache. Use a scheduled task to check for any heartbeats that were stopped and evict them to prevent bloating.
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::removeAllStoppedHeartbeats, cleanupInterval, cleanupInterval, cleanupUnit);
    }

    /**
     * Associate the given {@link QueryHeartbeat} with its query ID in the cache. A listener will be set in the heartbeat that will notify this cache when
     * {@link QueryHeartbeat#stop()} is called and automatically evict the heartbeat from the cache.
     *
     * @param heartbeat
     *            the heartbeat
     */
    @Override
    public void put(QueryHeartbeat heartbeat) {
        // Add a listener to the heartbeat that will automatically trigger the heartbeat's eviction if it is ever stopped outside the cache's stop and remove
        // method.
        heartbeat.addListener(new HeartbeatStoppedListener(this));
        this.cache.put(heartbeat.getQueryId(), heartbeat);
    }

    /**
     * Return the {@link QueryHeartbeat} associated with the given query ID, or null if there is no cached {@link QueryHeartbeat} with the query ID.
     *
     * @param queryId
     *            the query ID
     * @return the heartbeat, possibly null
     */
    @Override
    public QueryHeartbeat get(String queryId) {
        return this.cache.getIfPresent(queryId);
    }

    /**
     * Return the set of query ID keys in the cache.
     *
     * @return the query IDs
     */
    @Override
    public Set<String> getQueryIds() {
        return cache.asMap().keySet();
    }

    /**
     * If any {@link QueryHeartbeat} values are stored in the cache for the given query IDs, remove them from the cache and stop them via
     * {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     *
     * @param queryIds
     *            the query IDs
     */
    @Override
    public void stopAndRemove(Collection<String> queryIds) {
        if (log.isTraceEnabled()) {
            log.trace("Stopping heartbeats for queries {}", queryIds);
        }

        if (queryIds != null && !queryIds.isEmpty()) {
            ConcurrentMap<String,QueryHeartbeat> map = this.cache.asMap();
            for (String queryId : queryIds) {
                QueryHeartbeat heartbeat = map.remove(queryId);
                stopHeartbeat(heartbeat);
            }
        }
    }

    /**
     * If a {@link QueryHeartbeat} is stored in the cache for the given query ID, remove it from the cache and stop the heartbeat via
     * {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void stopAndRemove(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Removing heartbeat for query {}", queryId);
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
     * Iterates through the entries of the cache and evicts all {@link QueryHeartbeat} instances that are stopped. This will be called on regular basis with the
     * cleanup interval that this {@link QueryHeartbeatCacheImpl} was configured with.
     */
    @Override
    public void removeAllStoppedHeartbeats() {
        Set<String> queryIds = new HashSet<>();
        // Collect the query IDs of all heartbeats that are considered stopped.
        for (QueryHeartbeat heartbeat : cache.asMap().values()) {
            if (heartbeat.isStopped()) {
                queryIds.add(heartbeat.getQueryId());
            }
        }
        // Invalidate them.
        this.cache.invalidateAll(queryIds);
    }

    /**
     * Closes this {@link QueryHeartbeatCacheImpl} and shuts down the scheduled cleanup task.
     */
    @PreDestroy
    public void shutdown() {
        log.debug("Shutting down");
        // Shut down the scheduler.
        try {
            if (this.scheduler != null) {
                this.scheduler.shutdown();
            }
        } catch (Exception e) {
            log.error("Error shutting down scheduled executor service", e);
        }

        // Clear the cache and stop all the heartbeats.
        try {
            this.cache.asMap().keySet().forEach(this::stopAndRemove);
        } catch (Exception e) {
            log.error("Error clearing heartbeat cache", e);
        }
    }

    /**
     * A simple listener that can be used to listen for when to evict a {@link QueryHeartbeat} from the cache.
     */
    private static class HeartbeatStoppedListener implements Consumer<String> {

        private final QueryHeartbeatCacheImpl cache;

        public HeartbeatStoppedListener(QueryHeartbeatCacheImpl cache) {
            this.cache = cache;
        }

        @Override
        public void accept(String queryId) {
            this.cache.stopAndRemove(queryId);
        }
    }
}
