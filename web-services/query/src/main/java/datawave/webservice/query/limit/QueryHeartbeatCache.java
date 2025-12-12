package datawave.webservice.query.limit;

import javax.inject.Singleton;

import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * A cache for storing query heartbeats of running queries.
 */
@Singleton
public class QueryHeartbeatCache {

    private static final Logger log = Logger.getLogger(QueryHeartbeatCache.class);

    private final Cache<String,QueryHeartbeat> cache = Caffeine.newBuilder().build();

    /**
     * Associate the given {@link QueryHeartbeat} with the given query ID in the cache. A listener will be set in the heartbeat that will notify this cache when
     * {@link QueryHeartbeat#stop()} is called and automatically evict the heartbeat from the cache.
     *
     * @param queryId
     *            the query ID
     * @param heartbeat
     *            the heartbeat
     */
    public void put(String queryId, QueryHeartbeat heartbeat) {
        // Add a listener to the heartbeat that will automatically trigger the heartbeat's eviction if it is ever stopped outside the cache's stop and remove
        // method.
        heartbeat.setListener(new HeartbeatStoppedListener(this));
        this.cache.put(queryId, heartbeat);
    }

    /**
     * Return the {@link QueryHeartbeat} associated with the given query ID, or null if there is no cached {@link QueryHeartbeat} with the query ID.
     *
     * @param queryId
     *            the query ID
     * @return the heartbeat, possibly null
     */
    public QueryHeartbeat get(String queryId) {
        return cache.getIfPresent(queryId);
    }

    /**
     * If a {@link QueryHeartbeat} is stored in the cache for the given query ID, remove it from the cache and stop the heartbeat via
     * {@link QueryHeartbeat#stop()}.
     *
     * @param queryId
     *            the query ID
     */
    public void stopAndRemoveHeartbeat(String queryId) {
        QueryHeartbeat heartbeat = cache.asMap().remove(queryId);
        if (heartbeat != null) {
            try {
                heartbeat.stopWithoutNotifyingListener();
            } catch (Exception e) {
                log.error("Error stopping heartbeat for query " + queryId, e);
            }
        }
    }

    /**
     * Clear the cache. This does not stop any of the heartbeats contained within the cache.
     */
    public void clear() {
        log.debug("Clearing internal cache");
        cache.asMap().clear();
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
