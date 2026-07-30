package datawave.webservice.query.limit;

import java.io.IOException;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.log4j.Logger;

/**
 * Represents a heartbeat for an active query. As long as the connection to Zookeeper is not disrupted, the heartbeat will persist and indicate that a query is
 * currently running.
 */
public class QueryHeartbeat {

    private static final Logger log = Logger.getLogger(QueryHeartbeat.class);

    private final String queryId;
    private final PersistentNode node;

    private QueryHeartbeatCache.HeartbeatStoppedListener listener;

    public QueryHeartbeat(String queryId, PersistentNode node) {
        Objects.requireNonNull(queryId, "Parameter queryId must not be null");
        Objects.requireNonNull(node, "Parameter node must not be null");
        this.queryId = queryId;
        this.node = node;
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
    public PersistentNode getNode() {
        return node;
    }

    /**
     * Stop and delete the heartbeat.
     *
     * @throws IOException
     *             if an error occurs while deleting the heartbeat.
     */
    public void stop() throws IOException {
        stopWithoutNotifyingListener();
        if (this.listener != null) {
            this.listener.heartbeatStopped(this.queryId);
        }
    }

    /**
     * Stop and delete the heartbeat without notifying the internal listener. This is used by {@link QueryHeartbeatCache} to avoid necessary looping calls.
     */
    public void stopWithoutNotifyingListener() {
        try {
            this.node.close();
        } catch (Exception e) {
            log.error("Error closing ephemeral node", e);
        }
    }

    /**
     * Return whether this {@link QueryHeartbeat} is considered stopped.
     *
     * @return true if the heartbeat is stopped, or false otherwise
     */
    public boolean isStopped() {
        return this.node.getActualPath() == null;
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

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        QueryHeartbeat heartbeat = (QueryHeartbeat) o;
        return Objects.equals(queryId, heartbeat.queryId) && Objects.equals(node, heartbeat.node) && Objects.equals(listener, heartbeat.listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, node, listener);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", QueryHeartbeat.class.getSimpleName() + "[", "]").add("queryId=" + queryId).add("node=" + node).add("listener=" + listener)
                        .toString();
    }
}
