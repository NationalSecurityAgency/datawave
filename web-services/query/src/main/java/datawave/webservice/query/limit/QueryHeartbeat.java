package datawave.webservice.query.limit;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.log4j.Logger;

import datawave.webservice.query.cache.QueryHeartbeatCache;

/**
 * Represents a heartbeat for an active query. As long as the connection to Zookeeper is not disrupted, the heartbeat will persist and indicate that a query is
 * currently running. Multiple heartbeats can be obtained for the same query.
 */
public class QueryHeartbeat {

    private static final Logger log = Logger.getLogger(QueryHeartbeat.class);

    private final String queryId;
    private final Collection<PersistentNode> nodes;

    private QueryHeartbeatCache.HeartbeatStoppedListener listener;

    public QueryHeartbeat(String queryId, Collection<PersistentNode> nodes) {
        Objects.requireNonNull(queryId, "Parameter queryId must not be null");
        Objects.requireNonNull(nodes, "Parameter node must not be null");
        this.queryId = queryId;
        this.nodes = Collections.unmodifiableCollection(nodes);
    }

    /**
     * Return the ID of the query this heartbeat is associated with.
     *
     * @return the query ID
     */
    public String getQueryId() {
        return queryId;
    }

    /**
     * Return the underlying nodes. The collection will be unmodifiable.
     *
     * @return the nodes
     */
    public Collection<PersistentNode> getNodes() {
        return nodes;
    }

    /**
     * Stop and delete the heartbeat.
     *
     * @throws IOException
     *             if an error occurs while deleting the heartbeat.
     */
    public void stop() throws IOException {
        stopWithoutNotifyingListener();
        if (listener != null) {
            listener.heartbeatStopped(queryId);
        }
    }

    /**
     * Stop and delete the heartbeat without notifying the internal listener. This is used by {@link QueryHeartbeatCache} to avoid necessary looping calls.
     */
    public void stopWithoutNotifyingListener() {
        for (PersistentNode node : nodes) {
            try {
                node.close();
            } catch (Exception e) {
                log.error("Error closing ephemeral node", e);
            }
        }
    }

    /**
     * Set a listener for this {@link QueryHeartbeat} to notify when {@link QueryHeartbeat#stop()} has been called.
     *
     * @param listener
     *            the listener
     */
    public void setListener(QueryHeartbeatCache.HeartbeatStoppedListener listener) {
        this.listener = listener;
    }
}
