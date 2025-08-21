package datawave.webservice.query.limit;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import java.io.IOException;
import java.util.Objects;

/**
 * Represents a heartbeat for an active query. As long as the connection to Zookeeper is not disrupted, the heartbeat will persist and indicate that a query is
 * currently running. Multiple heartbeats can be obtained for the same query.
 */
public class QueryHeartbeat {
    
    private final String queryId;
    private final PersistentNode node;
    
    public QueryHeartbeat(String queryId, PersistentNode node) {
        Objects.requireNonNull(queryId, "Parameter queryId must not be null");
        Objects.requireNonNull(node, "Parameter node must not be null");
        this.queryId = queryId;
        this.node = node;
    }
    
    /**
     * Return the ID of the query this heartbeat is associated with.
     * @return the query ID
     */
    public String getQueryId() {
        return queryId;
    }
    
    /**
     * Return the path to the heartbeat node. Possibly null if the node no longer exists.
     * @return the path
     */
    public String getPath() {
        return node.getActualPath();
    }
    
    /**
     * Stop and delete the heartbeat.
     */
    public void stop() throws IOException {
        node.close();
    }
}
