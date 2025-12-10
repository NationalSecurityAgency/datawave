package datawave.webservice.query.limit;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
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

    private static final String NULL_BYTE = "\0";
    private static final byte[] EMPTY_DATA = new byte[0];

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

    /**
     * Fetch a snapshot of actively running queries that meet at least one of the following criteria:
     * <ul>
     * <li>Submitted by the given user</li>
     * <li>Submitted on the given system</li>
     * <li>Submitted as one of the given query logics</li>
     * </ul>
     *
     * @param userDn
     *            the user DN
     * @param system
     *            the system
     * @param queryLogics
     *            the query logics to filter on
     * @return the snapshot
     * @throws Exception
     *             if an error occurs while compiling the snapshot
     */
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
                    String data = new String(client.getData().forPath(queryIdPath));
                    String[] parts = data.split(NULL_BYTE);
                    String userData = parts[0];
                    String systemData = parts[1];
                    String queryLogicData = parts[2];
                    builder.capture(queryId, userData, systemData, queryLogicData);
                } catch (KeeperException.NoNodeException e) {
                    // If a NoNodeException occurred when fetching the metadata for a particular query ID, it is likely that the nodes were deleted on a
                    // different after the query was considered no longer active.
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
     * Begin tracking an active query. All nodes will be created under the namespace {@value ZOOKEEPER_NAMESPACE}. The following nodes will be created as
     * containers.
     *
     * <pre>
     * /users/&lt;userDn&gt;
     * /systems/&lt;systemName&gt;
     * /queryLogics/&lt;queryLogic&gt;
     * /queries
     * /distinctQueryLogics
     * </pre>
     *
     * The following node will be created if it does not exist.
     *
     * <pre>
     * /distinctQueryLogics/&lt;queryLogic&gt;
     * </pre>
     *
     * The following nodes will be created as ephemeral nodes and will be closeable by the returned {@link QueryHeartbeat}.
     *
     * <pre>
     * /users/&lt;userDn&gt;/&lt;queryId&gt;
     * /systems/&lt;systemName&gt;/&lt;queryId&gt;
     * /queryLogics/&lt;queryLogic&gt;/&lt;queryId&gt;
     * /queries/&lt;queryId&gt;
     * </pre>
     * <p>
     *
     * The node /queries/&lt;queryId&gt; will have the data "&lt;userDn&gt;\0&lt;system&gt;\0&lt;queryLogic&gt;"
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
    public QueryHeartbeat trackQuery(String queryId, String userDn, String system, String queryLogic) throws Exception {
        // Normalize the userDN, system, and queryLogic.
        userDn = userDn.trim().toLowerCase();
        system = system.trim();
        queryLogic = queryLogic.trim();

        QueryHeartbeat heartbeat;

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
                    client.createContainers("/queries/");
                    client.createContainers("/systems/" + system);
                    client.createContainers("/users/" + userDn);
                    client.createContainers("/queryLogics/" + queryLogic);
                    client.createContainers("/distinctQueryLogics");

                    // Track the query logic as a distinct query logic if it isn't already.
                    try {
                        Stat distinctQueryLogicStat = client.checkExists().forPath("/distinctQueryLogics/" + queryLogic);
                        if (distinctQueryLogicStat == null) {
                            client.create().forPath("/distinctQueryLogics/" + queryLogic);
                        }
                    } catch (KeeperException.NodeExistsException e) {
                        // Do nothing, the queryLogic was tracked on another thread.
                    }

                    // Create ephemeral nodes that track information about the query. These nodes will not persist beyond the lifetime of the client created
                    // here.
                    CuratorFramework client = createClient();

                    List<PersistentNode> nodes = new ArrayList<>();
                    nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, "/users/" + userDn + "/" + queryId, EMPTY_DATA, false));
                    nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, "/systems/" + system + "/" + queryId, EMPTY_DATA, false));
                    nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, "/queryLogics/" + queryLogic + "/" + queryId, EMPTY_DATA, false));

                    String data = userDn + NULL_BYTE + system + NULL_BYTE + queryLogic;
                    nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, queryIdPath, data.getBytes(), false));

                    // Persist each node to Zookeeper.
                    for (PersistentNode node : nodes) {
                        node.start();
                        node.waitForInitialCreate(1, TimeUnit.SECONDS);
                    }

                    // Return the heartbeat.
                    heartbeat = new QueryHeartbeat(queryId, nodes);
                } else {
                    throw new QueryAlreadyTrackedException(queryId);
                }
            } catch (Exception e) {
                log.error("Failed to track query " + queryId, e);
                throw e;
            }
        } finally {
            clientLock.unlock();
        }
        return heartbeat;
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
