package datawave.webservice.query.limit;

import java.io.File;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * This class provides methods for leveraging Zookeeper to track queries and their active status.
 */
public class ActiveQueryTracker implements AutoCloseable {

    public static final String ZOOKEEPER_NAMESPACE = "ActiveQueries";

    private static final Logger log = Logger.getLogger(ActiveQueryTracker.class);

    private final String zookeeperConfig;
    private final long cleanUpClientInterval;
    private final Lock clientLock = new ReentrantLock();

    private CuratorFramework client;
    private long lastClientAccess;
    private Timer clientCleanupTimer;

    /**
     * Create and return a new {@link ActiveQueryTracker} instance
     *
     * @param zookeeperConfig
     *            the zookeeper config
     * @param clientCleanupInterval
     *            the interval in milliseconds after which the zookeeper client should be cleaned up since its last access\
     * @throws QuorumPeerConfig.ConfigException
     *             if an error occurs when verifying the zookeeper configuration
     */
    public ActiveQueryTracker(String zookeeperConfig, long clientCleanupInterval) throws QuorumPeerConfig.ConfigException {
        this.zookeeperConfig = getQuorumPeerConfig(zookeeperConfig);
        this.cleanUpClientInterval = clientCleanupInterval;
    }

    private static String getQuorumPeerConfig(String zookeeperConfig) throws QuorumPeerConfig.ConfigException {
        URI zookeeperConfigFile;
        try {
            zookeeperConfigFile = new Path(zookeeperConfig).toUri();
            if (new File(zookeeperConfigFile).exists()) {
                QuorumPeerConfig zooConfig = new QuorumPeerConfig();
                zooConfig.parse(zookeeperConfigFile.getPath());
                StringBuilder sb = new StringBuilder();
                for (QuorumPeer.QuorumServer server : zooConfig.getServers().values()) {
                    if (sb.length() > 0) {
                        sb.append(',');
                    }
                    sb.append(server.addr.getReachableOrOne().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                if (sb.length() == 0) {
                    sb.append(zooConfig.getClientPortAddress().getHostName()).append(':').append(zooConfig.getClientPortAddress().getPort());
                }
                return sb.toString();
            }
        } catch (IllegalArgumentException e) {
            // Try the zookeeper config as is.
        }
        return zookeeperConfig;
    }

    public ActiveQuerySnapshot getSnapshot(String userDn, String system, Set<String> queryLogics) throws Exception {
        if (log.isTraceEnabled()) {
            log.trace("Fetching snapshot for userDn=" + userDn + ", system=" + system + ", queryLogics=" + queryLogics + ")");
        }
        clientLock.lock();
        try {
            long currentTimeMillis = System.currentTimeMillis();
            initClient();

            Set<String> queryIds = new HashSet<>();
            // Fetch the ids of all queries submitted by the user.
            queryIds.addAll(getQueryIds(client, "/users/" + userDn));
            // Fetch the ids of all queries submitted on the system.
            queryIds.addAll(getQueryIds(client, "/systems/" + system));
            // Fetch the ids of all queries that have related query logics.
            for (String queryLogic : queryLogics) {
                queryIds.addAll(getQueryIds(client, "/queryLogics/" + queryLogic));
            }

            if (log.isTraceEnabled()) {
                log.trace("Found " + queryIds.size() + " active potentially related queries");
            }

            ActiveQuerySnapshot.Builder builder = ActiveQuerySnapshot.builder(userDn, system, queryLogics).withTimestamp(currentTimeMillis);
            // Fetch the metadata of each query and capture them if considered to be active.
            for (String queryId : queryIds) {
                String queryIdPath = "/queries/" + queryId;
                try {
                    // Only capture the current query ID as an active query if there is at least one heartbeat.
                    if (!client.getChildren().forPath(queryIdPath + "/heartbeats").isEmpty()) {
                        String userData = new String(client.getData().forPath(queryIdPath + "/user"));
                        String systemData = new String(client.getData().forPath(queryIdPath + "/system"));
                        String queryLogicData = new String(client.getData().forPath(queryIdPath + "/queryLogic"));
                        builder.capture(queryId, userData, systemData, queryLogicData);
                    }
                } catch (KeeperException.NoNodeException e) {
                    // If a NoNodeException occurred when fetching the metadata for a particular query ID, it is likely that the query was untracked by another
                    // ActiveQueryTracker instance in the time between obtaining the query and now. Simply skip over it.
                    if (log.isTraceEnabled()) {
                        log.trace("Skipping capture of queryId=" + queryId + " in snapshot, nodes potentially deleted during scan");
                    }
                }
            }
            return builder.build();
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Return the list of children for the given container, or an empty list if the container does not exist.
     *
     * @param client
     *            the client
     * @param containerPath
     *            the path to the container
     * @return the list of query ids
     * @throws Exception
     *             if an error occurs
     */
    private List<String> getQueryIds(CuratorFramework client, String containerPath) throws Exception {
        try {
            return client.getChildren().forPath(containerPath);
        } catch (KeeperException.NoNodeException e) {
            // If a NoNodeException was thrown here, it occurred because there are no queries being tracked anymore, and the container node was automatically
            // cleaned up as a result of having no children. Simply return an empty list.
            return List.of();
        }
    }

    /**
     * Begin tracking an active query. The following ZNodes will be created in Zookeeper under the namespace {@value ZOOKEEPER_NAMESPACE}:
     *
     * <pre>
     * /users/&lt;userDn&gt;/&lt;queryId&gt;
     * /systems/&lt;systemName&gt;/&lt;queryId&gt;
     * /queryLogics/&lt;queryLogic&gt;/&lt;queryId&gt;
     * /distinctQueryLogics/&lt;queryLogic&gt
     * /queries/&lt;queryId&gt;
     * /queries/&lt;queryId&gt;/user           [data = byte[] value of userDn]
     * /queries/&lt;queryId&gt;/system         [data = byte[] value of systemName]
     * /queries/&lt;queryId&gt;/queryLogic     [data = byte[] value of queryLogic]
     * /queries/&lt;queryId&gt;/heartbeats
     * </pre>
     * <p>
     * The paths
     *
     * <pre>
     * /users/&lt;userDn&gt;/&lt;queryId&gt;
     * /systems/&lt;systemName&gt;/&lt;queryId&gt;
     * /queryLogics/&lt;queryLogic&gt;/&lt;queryId&gt;
     * /queries/&lt;queryId&gt;
     * /distinctQueryLogics
     * </pre>
     * <p>
     * will be created as containers, and thus will become eligible for cleanup when they have no children.
     *
     * @param queryId
     *            the query's id
     * @param userDn
     *            the user who submitted the query
     * @param system
     *            the system the query was submitted on
     * @param queryLogic
     *            the query logic of the query
     */
    public void trackQuery(String queryId, String userDn, String system, String queryLogic) {
        // Normalize the userDN, system, and queryLogic.
        userDn = userDn.trim().toLowerCase();
        system = system.trim();
        queryLogic = queryLogic.trim();

        if (log.isTraceEnabled()) {
            log.trace("Tracking query: queryId=" + queryId + ", user='" + userDn + "', system=" + system + ", queryLogic=" + queryLogic);
        }

        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();

            try {
                String queryIdPath = "/queries/" + queryId;
                Stat stat = client.checkExists().forPath(queryIdPath);
                if (stat == null) {
                    // Ensure that we create following container nodes.
                    client.createContainers(queryIdPath);
                    client.createContainers("/systems/" + system);
                    client.createContainers("/users/" + userDn);
                    client.createContainers("/queryLogics/" + queryLogic);
                    client.createContainers("/distinctQueryLogics");

                    // Populate the information for the specific query ID atomically as a single transaction.
                    CuratorOp addQueryIdToUsers = client.transactionOp().create().forPath("/users/" + userDn + "/" + queryId);
                    CuratorOp addQueryIdToSystems = client.transactionOp().create().forPath("/systems/" + system + "/" + queryId);
                    CuratorOp addQueryIdToQueryLogics = client.transactionOp().create().forPath("/queryLogics/" + queryLogic + "/" + queryId);
                    CuratorOp addUserData = client.transactionOp().create().forPath(queryIdPath + "/user", userDn.getBytes());
                    CuratorOp addSystemData = client.transactionOp().create().forPath(queryIdPath + "/system", system.getBytes());
                    CuratorOp addQueryLogicData = client.transactionOp().create().forPath(queryIdPath + "/queryLogic", queryLogic.getBytes());
                    CuratorOp addHeartbeatsNode = client.transactionOp().create().forPath(queryIdPath + "/heartbeats");

                    client.transaction().forOperations(addQueryIdToUsers, addQueryIdToSystems, addQueryIdToQueryLogics, addUserData, addSystemData,
                                    addQueryLogicData, addHeartbeatsNode);

                    // Track the query logic as a distinct query logic if it isn't already.
                    try {
                        Stat distinctQueryLogicStat = client.checkExists().forPath("/distinctQueryLogics/" + queryLogic);
                        if (distinctQueryLogicStat == null) {
                            client.create().forPath("/distinctQueryLogics/" + queryLogic);
                        }
                    } catch (KeeperException.NodeExistsException e) {
                        // Do nothing, the node was created on another thread.
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("Unable to track query " + queryId, e);
            }
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Stop tracking a query that is no longer considered to be active. The following ZNodes will be deleted in Zookeeper under the namespace
     * {@value ZOOKEEPER_NAMESPACE}:
     *
     * <pre>
     * /queries/&lt;queryId&gt;
     * /queries/&lt;queryId&gt;/user
     * /queries/&lt;queryId&gt;/system
     * /queries/&lt;queryId&gt;/queryLogic
     * /queries/&lt;queryId&gt;/heartbeats
     * /users/&lt;userDn&gt;/&lt;queryId&gt;
     * /systems/&lt;systemName&gt;/&lt;queryId&gt;
     * /queryLogics/&lt;queryLogic&gt;/&lt;queryId&gt;
     * </pre>
     *
     * @param queryId
     *            the query's id
     * @throws ActiveQueryException
     *             if the node {@code /queries/<queryId>/heartbeats} has any children indicating that the query is considered to be active
     */
    public void stopTrackingQuery(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Stopping tracking of query: queryId=" + queryId);
        }

        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();

            String queryIdPath = "/queries/" + queryId;
            Stat stat = client.checkExists().forPath(queryIdPath);
            // If the query id node exists, delete all information for the relevant
            if (stat != null) {
                // Do not allow the query to be untracked if any heartbeat nodes current exist.
                String heartbeatsPath = queryIdPath + "/heartbeats";
                Stat heartbeatsStat = client.checkExists().forPath(heartbeatsPath);
                if (heartbeatsStat != null && heartbeatsStat.getNumChildren() > 0) {
                    throw new ActiveQueryException("Cannot stop tracking query " + queryId + ", " + heartbeatsStat.getNumChildren() + " heartbeat(s) exist");
                }

                // Delete the information for the specific query ID atomically as a single transaction.
                String userDn = new String(client.getData().forPath(queryIdPath + "/user"));
                String system = new String(client.getData().forPath(queryIdPath + "/system"));
                String queryLogic = new String(client.getData().forPath(queryIdPath + "/queryLogic"));

                CuratorOp deleteQueryInUsers = client.transactionOp().delete().forPath("/users/" + userDn + "/" + queryId);
                CuratorOp deleteQueryInSystems = client.transactionOp().delete().forPath("/systems/" + system + "/" + queryId);
                CuratorOp deleteQueryInQueryLogics = client.transactionOp().delete().forPath("/queryLogics/" + queryLogic + "/" + queryId);
                CuratorOp deleteQueryHeartbeats = client.transactionOp().delete().forPath(heartbeatsPath);
                CuratorOp deleteQueryUser = client.transactionOp().delete().forPath(queryIdPath + "/user");
                CuratorOp deleteQuerySystem = client.transactionOp().delete().forPath(queryIdPath + "/system");
                CuratorOp deleteQueryQueryLogic = client.transactionOp().delete().forPath(queryIdPath + "/queryLogic");
                CuratorOp deleteQueryIdNode = client.transactionOp().delete().forPath(queryIdPath);
                client.transaction().forOperations(deleteQueryInUsers, deleteQueryInSystems, deleteQueryInQueryLogics, deleteQueryHeartbeats, deleteQueryUser,
                                deleteQuerySystem, deleteQueryQueryLogic, deleteQueryIdNode);
            }
        } catch (ActiveQueryException e) {
            log.error("Failed to stop tracking query " + queryId, e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to stop tracking of query: queryId=" + queryId, e);
            throw new ActiveQueryException("Failed to clean up nodes for query " + queryId, e);
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Return a new {@link QueryHeartbeat} for the given queryId. This will result in the creation of a new sequential, ephemeral node as a child of the node
     * {@code /queries/<queryId>/heartbeats} under the namespace {@value ZOOKEEPER_NAMESPACE}.
     *
     * @param queryId
     *            the queryId
     * @return the heartbeat
     * @throws ActiveQueryException
     *             if the query is not being tracked
     */
    public QueryHeartbeat createHeartbeat(String queryId) {
        if (log.isTraceEnabled()) {
            log.trace("Obtaining heartbeat for queryId=" + queryId);
        }

        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();

            // Make sure the query is being tracked.
            String queryIdPath = "/queries/" + queryId;
            Stat stat = client.checkExists().forPath(queryIdPath);
            if (stat == null) {
                throw new ActiveQueryException("Query " + queryId + " is not being tracked");
            }

            // Prepare the heartbeat node.
            String heartbeatPath = queryIdPath + "/heartbeats/heartbeat_";
            CuratorFramework client = createClient();
            PersistentNode node = new PersistentNode(client, CreateMode.EPHEMERAL_SEQUENTIAL, true, heartbeatPath, "".getBytes(), false);

            // Create the actual node, with the given wait for creation if needed. This will block until the node creation from start() exceeds, or until the
            // wait time elapses, whichever comes first.
            node.start();
            node.waitForInitialCreate(5, TimeUnit.SECONDS);

            if (log.isTraceEnabled()) {
                log.trace("Created heartbeat node " + node.getActualPath() + " for queryId=" + queryId);
            }
            return new QueryHeartbeat(queryId, node);
        } catch (ActiveQueryException e) {
            log.error("Failed to create heartbeat for queryId=" + queryId, e);
            throw e;
        } catch (Exception e) {
            log.error("Failed to create heartbeat for queryId=" + queryId, e);
            throw new ActiveQueryException("Unable to obtain heartbeat for queryId=" + queryId, e);
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Return the set of distinct query logics that have been tracked at some point while Zookeeper has been up.
     *
     * @return the query logics
     */
    public Set<String> getDistinctQueryLogics() {
        if (log.isTraceEnabled()) {
            log.trace("Obtaining distinct query logics");
        }
        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();
            // If any query logics were tracked, return them.
            Stat stat = client.checkExists().forPath("/distinctQueryLogics");
            if (stat != null) {
                return Set.copyOf(client.getChildren().forPath("/distinctQueryLogics"));
            } else {
                // Otherwise return an empty set.
                return Set.of();
            }
        } catch (Exception e) {
            log.error("Failed to fetch distinct query logics", e);
            throw new ActiveQueryException(e);
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Initialize the zookeeper client and cleanup timer if not already initialize. Calling this method will update the last time the client was accessed to the
     * current time.
     */
    private void initClient() {
        if (client == null) {
            clientLock.lock();
            try {
                // @formatter:off
                client = createClient();
                if (cleanUpClientInterval > 0) {
                    createCleanupTimer();
                }
            } finally {
                clientLock.unlock();
            }
        }
        // Update the last time the client was accessed.
        lastClientAccess = System.currentTimeMillis();
    }

    /**
     * Return a new zookeeper client targeting the namespace {@value #ZOOKEEPER_NAMESPACE}.
     * @return the client
     */
    private CuratorFramework createClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .namespace(ZOOKEEPER_NAMESPACE)
                        .connectString(zookeeperConfig)
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();

        // @formatter:on
        client.start();
        return client;
    }

    /**
     * Create the cleanup timer.
     */
    private void createCleanupTimer() {
        if (clientCleanupTimer == null) {
            clientCleanupTimer = new Timer("Zookeeper Client Cleanup");
        }

        clientCleanupTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                if (lastClientAccess + cleanUpClientInterval <= System.currentTimeMillis()) {
                    cancel();
                } else if (client == null) {
                    cancel();
                }
            }
        }, cleanUpClientInterval, cleanUpClientInterval);
    }

    /**
     * Clean up the underlying resources used by this {@link ActiveQueryTracker}.
     */
    public void cleanup() {
        closeClientAndTimer();
    }

    /**
     * Close the client and clean up timer, and nullify them.
     */
    private void closeClientAndTimer() {
        if (client != null) {
            clientLock.lock();
            try {
                if (clientCleanupTimer != null) {
                    clientCleanupTimer.cancel();
                    clientCleanupTimer = null;
                }
                if (client != null) {
                    try {
                        client.close();
                    } finally {
                        client = null;
                    }
                }
            } finally {
                clientLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        cleanup();
    }
}
