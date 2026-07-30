package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.QUERIES_ROOT_PATH;
import static datawave.webservice.query.limit.QueryLimiterUtils.QUERY_LOGICS_ROOT_PATH;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import datawave.zookeeper.ZkLock;

/**
 * This class provides methods for leveraging Zookeeper to track queries and their active status.
 */
public class ActiveQueryTracker {

    private static final Logger log = Logger.getLogger(ActiveQueryTracker.class);

    /**
     * The Zookeeper client.
     */
    private final CuratorFramework client;

    /**
     * The distributed lock.
     */
    private final ZkLock zkLock;

    public ActiveQueryTracker(CuratorFramework client) {
        this.client = client;
        this.zkLock = new ZkLock(client, "/locks", 30, TimeUnit.SECONDS);
    }

    /**
     * Begin tracking an active query. All nodes will be created under the namespace {@value QueryLimiterUtils#ZOOKEEPER_NAMESPACE}.
     * <ul>
     * <li>The node {@code /queryLogics/<queryLogic>} will be created with empty data if it does not already exist.</li>
     * <li>An ephemeral node {@code /queries/<queryId>} will be created with data consisting of a byte array with the user DN, system, and query logic written
     * to it. The node will be closeable by the returned {@link QueryHeartbeat}.</li>
     * </ul>
     *
     * @param queryId
     *            the query's ID
     * @param userDn
     *            the DN of the user who submitted the query
     * @param system
     *            the system the query was submitted on
     * @param queryLogic
     *            the query logic of the query
     * @return a new query heartbeat that can close the ephemeral nodes associated with the query
     * @throws QueryAlreadyTrackedException
     *             if the given query ID is already being tracked as an active query
     */
    public QueryHeartbeat trackQuery(String queryId, String userDn, String system, String queryLogic) {
        if (log.isTraceEnabled()) {
            log.trace("Tracking query: queryId=" + queryId + ", user='" + userDn + "', system='" + system + "', queryLogic='" + queryLogic + "'");
        }

        try {
            return zkLock.callWithLock(queryId, () -> trackQueryWithLock(queryId, userDn, system, queryLogic));
        } catch (QueryLimiterException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryLimiterException("Error tracking query " + queryId, e);
        }
    }

    private QueryHeartbeat trackQueryWithLock(String queryId, String userDn, String system, String queryLogic) throws Exception {
        // Verify that the query is not already being tracked.
        verifyQueryIsNotTracked(queryId);

        // Track the query logic as a distinct query logic if not already tracked.
        trackDistinctQueryLogic(queryLogic);

        // Create and return the query heartbeat.
        return createHeartbeat(queryId, userDn, system, queryLogic);
    }

    /**
     * Verify a node is not already present with the given query ID.
     *
     * @param queryId
     *            the query ID
     * @throws Exception
     *             if an error occurs with Zookeeper
     * @throws QueryAlreadyTrackedException
     *             if the query is already tracked
     */
    private void verifyQueryIsNotTracked(String queryId) throws Exception {
        // Verify we are not already tracking the query.
        String queryIdPath = getQueryIdPath(queryId);
        Stat stat = client.checkExists().forPath(queryIdPath);
        if (stat != null) {
            throw new QueryAlreadyTrackedException(queryId);
        }
    }

    /**
     * Create the node {@code /queryLogics/<queryLogic>} if it does not already exist.
     *
     * @param queryLogic
     *            the query logic
     * @throws Exception
     *             if an error occurs with Zookeeper
     */
    private void trackDistinctQueryLogic(String queryLogic) throws Exception {
        try {
            client.create().creatingParentsIfNeeded().forPath(getQueryLogicPath(queryLogic));
        } catch (KeeperException.NodeExistsException e) {
            // Do nothing, the queryLogic was tracked on another thread.
        }
    }

    /**
     * Create and return a new {@link QueryHeartbeat} that contains an ephemeral node closeable by the heartbeat. The node will have the path
     * {@code /queries/<queryId>}, and its data will consist of the userDn, system, and queryLogic written to it. The node will not persist beyond the lifetime
     * of the client fo this {@link ActiveQueryTracker}.
     *
     * @param queryId
     *            the query ID
     * @param userDn
     *            the user DN
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @return the new heartbeat
     * @throws InterruptedException
     *             if the thread is interrupted while waiting for the ephemeral node to be created
     */
    private QueryHeartbeat createHeartbeat(String queryId, String userDn, String system, String queryLogic) throws InterruptedException {
        byte[] data = createData(userDn, system, queryLogic);
        PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL, false, getQueryIdPath(queryId), data, true);

        // Start the node and ensure it is created before returning the heartbeat.
        node.start();
        node.waitForInitialCreate(1, TimeUnit.SECONDS);

        return new QueryHeartbeat(queryId, node);
    }

    /**
     * Return the path {@code /queryLogics/<queryLogic>}
     *
     * @param queryLogic
     *            the query logic
     * @return the path
     */
    private static String getQueryLogicPath(String queryLogic) {
        return QUERY_LOGICS_ROOT_PATH + "/" + queryLogic;
    }

    /**
     * Return the path {@code /queries/<queryId>}
     *
     * @param queryId
     *            the query ID
     * @return the path
     */
    private static String getQueryIdPath(String queryId) {
        return QUERIES_ROOT_PATH + "/" + queryId;
    }

    /**
     * Create a byte array that has the given user DN, system, and query logic written to it.
     *
     * @param userDn
     *            the user DN
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @return the byte array
     */
    private static byte[] createData(String userDn, String system, String queryLogic) {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            DataOutput dataOutput = new DataOutputStream(stream);
            WritableUtils.writeString(dataOutput, userDn);
            WritableUtils.writeString(dataOutput, system);
            WritableUtils.writeString(dataOutput, queryLogic);
            return stream.toByteArray();
        } catch (Exception e) {
            throw new QueryLimiterException("Failed to create data byte array", e);
        }
    }
}
