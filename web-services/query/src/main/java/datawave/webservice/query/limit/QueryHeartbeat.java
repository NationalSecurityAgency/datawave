package datawave.webservice.query.limit;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.log4j.Logger;

/**
 * Represents a heartbeat for an active query. As long as the connection to Zookeeper is not disrupted, the heartbeat will persist and indicate that a query is
 * currently running. Multiple heartbeats can be obtained for the same query.
 */
public class QueryHeartbeat {

    private static final Logger log = Logger.getLogger(QueryHeartbeat.class);

    private final String queryId;
    private final List<PersistentNode> nodes;

    public QueryHeartbeat(String queryId, List<PersistentNode> nodes) {
        Objects.requireNonNull(queryId, "Parameter queryId must not be null");
        Objects.requireNonNull(nodes, "Parameter node must not be null");
        this.queryId = queryId;
        this.nodes = List.copyOf(nodes);
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
     * Stop and delete the heartbeat.
     *
     * @throws IOException
     *             if an error occurs while deleting the heartbeat.
     */
    public void stop() throws IOException {
        for (PersistentNode node : nodes) {
            try {
                node.close();
            } catch (Exception e) {
                log.error("Error closing persistent node", e);
            }
        }
    }
}
