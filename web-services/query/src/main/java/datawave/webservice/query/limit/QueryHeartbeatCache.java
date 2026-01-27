package datawave.webservice.query.limit;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A cache for storing query heartbeats of running queries.
 */
public class QueryHeartbeatCache {

    private static final Logger log = Logger.getLogger(QueryHeartbeatCache.class);

    private final Cache<String,QueryHeartbeat> cache = Caffeine.newBuilder().removalListener((key, value, cause) -> {
        if (cause.wasEvicted()) {
            log.debug("Evicted heartbeat for query " + key);
        }
    }).build();

    private long cleanupInterval = 10;
    private TimeUnit cleanupUnit = TimeUnit.MINUTES;
    private ScheduledExecutorService scheduler;
    
    /**
     * Return the interval for which {@link QueryHeartbeatCache#removeAllStoppedHeartbeats()} will be called.
     * @return the interval
     */
    public long getCleanupInterval() {
        return cleanupInterval;
    }
    
    /**
     * Set the interval for which {@link QueryHeartbeatCache#removeAllStoppedHeartbeats()} should be called.
     * @param cleanupInterval the interval
     */
    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }
    
    /**
     * Return the time unit of the cleanup interval.
     * @return the time unit
     */
    public TimeUnit getCleanupUnit() {
        return cleanupUnit;
    }
    
    /**
     * Set the time unit of the cleanup interval.
     * @param cleanupUnit the cleanup interval time unit
     */
    public void setCleanupUnit(TimeUnit cleanupUnit) {
        this.cleanupUnit = cleanupUnit;
    }

    /**
     * Set up this heartbeat cache.
     */
    public void setup() {
        // If the PersistentNodes within a QueryHeartbeat are ever stopped due to a connection failure, and not via to QueryHeartbeat.stop(), the heartbeat will
        // not automatically evict itself from the cache. Use a scheduled task to check for any heartbeats that were stopped and evict them to prevent bloating.
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::removeAllStoppedHeartbeats, cleanupInterval, cleanupInterval, cleanupUnit);

        if (log.isDebugEnabled()) {
            log.debug("Initializing with cleanup schedule that will run every " + cleanupInterval + " " + cleanupUnit);
        }
    }

    /**
     * Associate the given {@link QueryHeartbeat} with its query ID in the cache. A listener will be set in the heartbeat that will notify this cache when
     * {@link QueryHeartbeat#stop()} is called and automatically evict the heartbeat from the cache.
     *
     * @param heartbeat
     *            the heartbeat
     */
    public void put(QueryHeartbeat heartbeat) {
        // Add a listener to the heartbeat that will automatically trigger the heartbeat's eviction if it is ever stopped outside the cache's stop and remove
        // method.
        heartbeat.setListener(new HeartbeatStoppedListener(this));
        this.cache.put(heartbeat.getQueryId(), heartbeat);
    }

    /**
     * Return the {@link QueryHeartbeat} associated with the given query ID, or null if there is no cached {@link QueryHeartbeat} with the query ID.
     *
     * @param queryId
     *            the query ID
     * @return the heartbeat, possibly null
     */
    public QueryHeartbeat get(String queryId) {
        return this.cache.getIfPresent(queryId);
    }
    
    /**
     * Return the set of query ID keys in the cache.
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
     */
    public void stopAndRemoveHeartbeats(Set<String> queryIds) {
        if(log.isTraceEnabled()) {
            log.trace("Stopping heartbeats for queries " + queryIds);
        }
        
        if(queryIds != null && !queryIds.isEmpty()) {
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
    public void stopAndRemoveHeartbeat(String queryId) {
        if(log.isTraceEnabled()) {
            log.trace("Removing heartbeat for query " + queryId);
        }
        
        QueryHeartbeat heartbeat = this.cache.asMap().remove(queryId);
        stopHeartbeat(heartbeat);
    }
    
    /**
     * Attempt to stop the given heartbeat via {@link QueryHeartbeat#stopWithoutNotifyingListener()}.
     * @param heartbeat the heartbeat to stop
     */
    private void stopHeartbeat(QueryHeartbeat heartbeat) {
        if(heartbeat != null) {
            try {
                heartbeat.stopWithoutNotifyingListener();
            } catch (Exception e) {
                log.error("Error stopping heartbeat for query " + heartbeat.getQueryId(), e);
            }
        }
    }

    /**
     * Iterates through the entries of the cache and evicts all {@link QueryHeartbeat} instances that are stopped. This will be called on regular basis with the
     * cleanup interval that this {@link QueryHeartbeatCache} was configured with.
     */
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
     * Closes this {@link QueryHeartbeatCache} and shuts down the scheduled cleanup task.
     */
    public void shutdown() {
        log.debug("Shutting down");
        try {
            if (this.scheduler != null) {
                this.scheduler.shutdown();
            }
        } catch (Exception e) {
            log.error("Error shutting down scheduled executor service", e);
        }

        // Clear the cache and stop all the heartbeats.
        try {
            this.cache.asMap().keySet().forEach(this::stopAndRemoveHeartbeat);
        } catch (Exception e) {
            log.error("Error clearing heartbeat cache", e);
        }
    }
    
    /**
     * A simple listener that can be used to listen for when to evict a {@link QueryHeartbeat} from the cache.
     */
    public static class HeartbeatStoppedListener {

        private final QueryHeartbeatCache cache;

        public HeartbeatStoppedListener(QueryHeartbeatCache cache) {
            this.cache = cache;
        }

        /**
         * Notifies the listener that the {@link QueryHeartbeat} associated with the given query ID has been stopped. Any cached mapping for the query ID will
         * be removed.
         *
         * @param queryId
         *            the query ID
         */
        public void heartbeatStopped(String queryId) {
            this.cache.stopAndRemoveHeartbeat(queryId);
        }
    }
}
