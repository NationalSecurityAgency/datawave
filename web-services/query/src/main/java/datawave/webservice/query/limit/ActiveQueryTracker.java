package datawave.webservice.query.limit;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Function;

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

    private static final byte[] EMPTY_DATA = new byte[0];

    private static final String DISTINCT_QUERY_LOGICS_CONTAINER_PATH = "/distinctQueryLogics";
    private static final String SYSTEMS_CONTAINER_PATH = "/systems";
    private static final String USERS_CONTAINER_PATH = "/users";

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
     *            the interval in milliseconds after which the zookeeper client should be cleaned up since its last access
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
     * Begin tracking an active query. All nodes will be created under the namespace {@value ZOOKEEPER_NAMESPACE}. The following nodes will be created as
     * containers.
     *
     * <pre>
     * /users/&lt;userDn&gt;/&lt;queryLogic&gt; (Created only if systemCountsAgainstUserLimit is true)
     * /systems/&lt;systemName&gt;/&lt;queryLogic&gt;
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
     * /users/&lt;userDn&gt;/&lt;queryLogic&gt;/&lt;queryId&gt; (Created only if systemCountsAgainstUserLimit is true)
     * /systems/&lt;systemName&gt;/&lt;queryLogic&gt;/&lt;queryId&gt;
     * </pre>
     *
     * @param queryId
     *            the query's ID
     * @param userDn
     *            the DN of the user who submitted the query
     * @param system
     *            the system the query was submitted on
     * @param queryLogic
     *            the query logic of the query
     * @param systemCountsAgainstUserLimit
     *            whether queries on the system count towards the user limit
     * @return a new query heartbeat that can close the ephemeral nodes associated with the query
     */
    public QueryHeartbeat trackQuery(String queryId, String userDn, String system, String queryLogic, boolean systemCountsAgainstUserLimit) throws Exception {
        // Normalize the userDN, system, and queryLogic.
        userDn = userDn.trim().toLowerCase();
        system = system.trim();
        queryLogic = queryLogic.trim();

        QueryHeartbeat heartbeat;

        if (log.isTraceEnabled()) {
            log.trace("Tracking query: queryId=" + queryId + ", user='" + userDn + "', system='" + system + "', queryLogic='" + queryLogic + "'");
        }

        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();
            try {
                // Verify we are not already tracking the query.
                String systemQueryIdPath = getSystemQueryIdPath(system, queryLogic, queryId);
                Stat stat = client.checkExists().forPath(systemQueryIdPath);
                if (stat != null) {
                    throw new QueryAlreadyTrackedException(queryId);
                }

                // Ensure we create the following container nodes.
                client.createContainers(getSystemQueryLogicPath(system, queryLogic));
                client.createContainers(DISTINCT_QUERY_LOGICS_CONTAINER_PATH);
                // Create the user DN containers only if the system counts against the user limit.
                if (systemCountsAgainstUserLimit) {
                    client.createContainers(getUserQueryLogicPath(userDn, queryLogic));
                }

                // Track the query logic as a distinct query logic if it isn't already.
                try {
                    String distinctQueryLogicPath = getDistinctQueryLogicPath(queryLogic);
                    Stat distinctQueryLogicStat = client.checkExists().forPath(distinctQueryLogicPath);
                    if (distinctQueryLogicStat == null) {
                        client.create().forPath(distinctQueryLogicPath);
                    }
                } catch (KeeperException.NodeExistsException e) {
                    // Do nothing, the queryLogic was tracked on another thread.
                }

                // Create ephemeral nodes for the query ID. These nodes will not persist beyond the lifetime of the client created here.
                CuratorFramework client = createClient();
                List<PersistentNode> nodes = new ArrayList<>();
                nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, systemQueryIdPath, EMPTY_DATA, false));
                if (systemCountsAgainstUserLimit) {
                    String userQueryIdPath = getUserQueryIdPath(userDn, queryLogic, queryId);
                    nodes.add(new PersistentNode(client, CreateMode.EPHEMERAL, false, userQueryIdPath, EMPTY_DATA, false));
                }

                // Persist each node to Zookeeper.
                for (PersistentNode node : nodes) {
                    node.start();
                    node.waitForInitialCreate(1, TimeUnit.SECONDS);
                }

                // Initialize the heartbeat.
                heartbeat = new QueryHeartbeat(queryId, nodes);

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
     * Return the total number of active running queries (on systems that count towards the user limit) for the given query logic that were submitted by the
     * given user.
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @return the total active running queries
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    public int getTotalUserQueriesForQueryLogic(String userDn, String queryLogic) throws Exception {
        // Normalize the user DN.
        userDn = userDn.trim().toLowerCase();

        if (log.isTraceEnabled()) {
            log.trace("Fetching total queries for user='" + userDn + "', queryLogic='" + queryLogic + "'");
        }

        return getTotalChildrenWithLock(getUserQueryLogicPath(userDn, queryLogic));
    }

    /**
     * Return the total number of active running queries for the given query logic that were submitted on the given system.
     *
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @return the total active running queries
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    public int getTotalSystemQueriesForQueryLogic(String system, String queryLogic) throws Exception {
        // Normalize the system.
        system = system.trim();

        if (log.isTraceEnabled()) {
            log.trace("Fetching total queries for system='" + system + "', queryLogic='" + queryLogic + "'");
        }

        return getTotalChildrenWithLock(getSystemQueryLogicPath(system, queryLogic));
    }

    /**
     * Obtain a lock for the client and return the total children for the given path. If the path does not exist, 0 will be returned.
     *
     * @param path
     *            the node path
     * @return the total children
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    private int getTotalChildrenWithLock(String path) throws Exception {
        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();

            return getTotalChildren(path);
        } catch (Exception e) {
            log.error("Failed to get total children for path " + path, e);
            throw e;
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Determine whether the total active queries for the given user meets or exceeds the given limit.
     *
     * @param userDn
     *            the user DN
     * @param queryLimit
     *            the query limit
     * @param queryLogicsAlreadyCounted
     *            the set of query logics that have already been counted towards the initial total
     * @param initialTotal
     *            the initial total already calculated previously when checking query logic limits
     * @return true if the total active queries meets or exceeds the given limit, or false otherwise
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    public boolean totalUserQueriesMeetsLimit(String userDn, int queryLimit, Set<String> queryLogicsAlreadyCounted, int initialTotal) throws Exception {
        // Normalize the user DN.
        userDn = userDn.trim().toLowerCase();

        if (log.isTraceEnabled()) {
            log.trace("Checking if total queries meets limit for user='" + userDn + "', queryLimit=" + queryLimit + ", queryLogicsAlreadyCounted="
                            + queryLogicsAlreadyCounted + ", initialTotal=" + initialTotal);
        }

        return totalQueriesMeetsLimit(userDn, queryLimit, queryLogicsAlreadyCounted, initialTotal, this::getUserPath, this::getUserQueryLogicPath);
    }

    /**
     * Determine whether the total active queries for the given system meets or exceeds the given limit.
     *
     * @param system
     *            the system
     * @param queryLimit
     *            the query limit
     * @param queryLogicsAlreadyCounted
     *            the set of query logics that have already been counted towards the initial total
     * @param initialTotal
     *            the initial total already calculated previously when checking query logic limits
     * @return true if the total active queries meets or exceeds the given limit, or false otherwise
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    public boolean totalSystemQueriesMeetsLimit(String system, int queryLimit, Set<String> queryLogicsAlreadyCounted, int initialTotal) throws Exception {
        // Normalize the system.
        system = system.trim();

        if (log.isTraceEnabled()) {
            log.trace("Checking if total queries meets limit for system='" + system + "', queryLimit=" + queryLimit + ", queryLogicsAlreadyCounted="
                            + queryLogicsAlreadyCounted + ", initialTotal=" + initialTotal);
        }

        return totalQueriesMeetsLimit(system, queryLimit, queryLogicsAlreadyCounted, initialTotal, this::getSystemPath, this::getSystemQueryLogicPath);
    }

    /**
     * Determine whether the total active queries for the given owner meets or exceeds the given limit.
     *
     * @param owner
     *            the owner ID, either a user DN or a system host name
     * @param limit
     *            the query limit
     * @param queryLogicsAlreadyCounted
     *            the set of query logics that have already been counted towards the initial total
     * @param initialTotal
     *            the initial total already calculated previously when checking query logic limits
     * @param ownerPathFunction
     *            the function to obtain the zookeeper path for the owner node
     * @param queryLogicPathFunction
     *            the function to obtain the zookeeper path for the query logic node under the owner node
     * @return true if the total active queries meets or exceeds the given limit, or false otherwise
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    private boolean totalQueriesMeetsLimit(String owner, int limit, Set<String> queryLogicsAlreadyCounted, int initialTotal,
                    Function<String,String> ownerPathFunction, BiFunction<String,String,String> queryLogicPathFunction) throws Exception {
        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();

            try {
                int total = initialTotal;
                // Get the set of query logic nodes underneath the owner node.
                List<String> queryLogics = client.getChildren().forPath(ownerPathFunction.apply(owner));
                for (String queryLogic : queryLogics) {
                    // If we have not already previously counted the total queries underneath the current query logic, increment the total by the number of
                    // queries for the current query logic.
                    if (!queryLogicsAlreadyCounted.contains(queryLogic)) {
                        String queryLogicPath = queryLogicPathFunction.apply(owner, queryLogic);
                        total += getTotalChildren(queryLogicPath);
                        // If the total is equal to or greater than the limit, return true.
                        if (total >= limit) {
                            return true;
                        }
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                // If this exception occurs, the owner node does not exist. There are no active queries for the owner. Simply return false.
                return false;
            } catch (Exception e) {
                log.error("Failed to count total queries for owner='" + owner + "'", e);
                throw e;
            }
        } finally {
            clientLock.unlock();
        }

        return false;
    }

    /**
     * Return the total number of children for the path. If the path does not exist, 0 will be returned.
     *
     * @param path
     *            the path
     * @return the total number of children
     * @throws Exception
     *             if an error occurs while scanning nodes
     */
    private int getTotalChildren(String path) throws Exception {
        try {
            Stat stat = client.checkExists().forPath(path);
            if (stat == null) {
                return 0;
            } else {
                return stat.getNumChildren();
            }
        } catch (Exception e) {
            log.error("Failed to obtain total children for path='" + path + "'", e);
            throw e;
        }
    }

    /**
     * Return the set of distinct query logics that have been tracked at some point while Zookeeper has been up.
     *
     * @return the query logics
     */
    public List<String> getDistinctQueryLogics() {
        if (log.isTraceEnabled()) {
            log.trace("Fetching distinct query logics");
        }
        clientLock.lock();
        try {
            // Initialize the client if needed.
            initClient();
            // If any query logics were tracked, return them.
            Stat stat = client.checkExists().forPath(DISTINCT_QUERY_LOGICS_CONTAINER_PATH);
            if (stat != null) {
                return client.getChildren().forPath(DISTINCT_QUERY_LOGICS_CONTAINER_PATH);
            } else {
                // Otherwise return an empty set.
                return List.of();
            }
        } catch (Exception e) {
            log.error("Failed to fetch distinct query logics", e);
            throw new ActiveQueryException(e);
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Return the path {@code /distinctQueryLogics/<queryLogic>}
     *
     * @param queryLogic
     *            the query logic
     * @return the path
     */
    private String getDistinctQueryLogicPath(String queryLogic) {
        return DISTINCT_QUERY_LOGICS_CONTAINER_PATH + "/" + queryLogic;
    }

    /**
     * Return the path {@code /users/<userDn>}
     *
     * @param userDn
     *            the user DN
     * @return the path
     */
    private String getUserPath(String userDn) {
        return USERS_CONTAINER_PATH + "/" + userDn;
    }

    /**
     * Return the path {@code /users/<userDn>/<queryLogic>}
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @return the path
     */
    private String getUserQueryLogicPath(String userDn, String queryLogic) {
        return getUserPath(userDn) + "/" + queryLogic;
    }

    /**
     * Return the path {@code /users/<userDn>/<queryLogic>/<queryId>}
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @param queryId
     *            the query ID
     * @return the path
     */
    private String getUserQueryIdPath(String userDn, String queryLogic, String queryId) {
        return getUserQueryLogicPath(userDn, queryLogic) + "/" + queryId;
    }

    /**
     * Return the path {@code /systems/<system>}
     *
     * @param system
     *            the system
     * @return the path
     */
    private String getSystemPath(String system) {
        return SYSTEMS_CONTAINER_PATH + "/" + system;
    }

    /**
     * Return the path {@code /systems/<system>/<queryLogic>}
     *
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @return the path
     */
    private String getSystemQueryLogicPath(String system, String queryLogic) {
        return getSystemPath(system) + "/" + queryLogic;
    }

    /**
     * Return the path {@code /systems/<system>/<queryLogic>/<queryId>}
     *
     * @param system
     *            the system
     * @param queryLogic
     *            the query logic
     * @param queryId
     *            the query ID
     * @return the path
     */
    private String getSystemQueryIdPath(String system, String queryLogic, String queryId) {
        return getSystemQueryLogicPath(system, queryLogic) + "/" + queryId;
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

    /**
     * Close this {@link ActiveQueryTracker} and call {@link #cleanup()}.
     */
    @Override
    public void close() {
        cleanup();
    }
}
