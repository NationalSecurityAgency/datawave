package datawave.webservice.query.cache;

import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;

import org.apache.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import datawave.webservice.query.limit.QueryHeartbeat;

/**
 * A cache for storing query heartbeats of running queries.
 */
@Singleton
public class QueryHeartbeatCache {

    private static final Logger log = Logger.getLogger(QueryHeartbeatCache.class);

    private final Cache<String,QueryHeartbeat> cache = Caffeine.newBuilder().build();

    public void put(String queryId, QueryHeartbeat heartbeat) {
        if (log.isDebugEnabled()) {
            log.debug("Adding heartbeat for query " + queryId + " to QueryHeartbeatCache");
        }
        cache.put(queryId, heartbeat);
    }

    public QueryHeartbeat get(String queryId) {
        return cache.getIfPresent(queryId);
    }

    public void stopAndRemoveHeartbeat(String queryId) {
        QueryHeartbeat heartbeat = cache.asMap().remove(queryId);
        if(log.isDebugEnabled()) {
            log.debug("Removed heartbeat for query " + queryId + " from QueryHeartbeatCache");
        }
        if (heartbeat != null) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Stopping heartbeat for query " + queryId);
                }
                heartbeat.stop();
            } catch (IOException e) {
                log.error("Error stopping query heartbeat", e);
            }
        }
    }

    public void clear() {
        log.debug("Clearing internal cache in QueryHeartbeatCache");
        cache.asMap().clear();
    }
}
