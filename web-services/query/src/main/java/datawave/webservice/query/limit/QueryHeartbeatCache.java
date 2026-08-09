package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.Set;

/**
 * A cache for {@link QueryHeartbeat} instances of actively running queries.
 */
public interface QueryHeartbeatCache {

    /**
     * Add the given heartbeat to the cache, mapping it to its query ID.
     *
     * @param heartbeat
     *            the heartbeat
     */
    void put(QueryHeartbeat heartbeat);

    /**
     * Get the {@link QueryHeartbeat} for the given query ID. Possibly null.
     *
     * @param queryId
     *            the query ID
     * @return the heartbeat
     */
    QueryHeartbeat get(String queryId);

    /**
     * Get the current set of query ID keys in the map.
     *
     * @return the query IDs
     */
    Set<String> getQueryIds();

    /**
     * Stop and remove the {@link QueryHeartbeat} for the given query ID.
     *
     * @param queryId
     *            the query ID
     */
    void stopAndRemove(String queryId);

    /**
     * Stop and remove the {@link QueryHeartbeat} for each of the given query IDs.
     *
     * @param queryIds
     *            the query IDs
     */
    void stopAndRemove(Collection<String> queryIds);

    /**
     * Remove all mappings where the {@link QueryHeartbeat} is stopped.
     */
    void removeAllStoppedHeartbeats();
}
