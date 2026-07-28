package datawave.webservice.query.limit;

import static datawave.webservice.query.limit.QueryLimiterUtils.ZOOKEEPER_NAMESPACE;
import static datawave.webservice.query.limit.QueryLimiterUtils.normalizeQueryLogic;
import static datawave.webservice.query.limit.QueryLimiterUtils.normalizeSystem;
import static datawave.webservice.query.limit.QueryLimiterUtils.normalizeUserDn;
import static datawave.zookeeper.ZkUtils.getQuorumPeerConfig;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;

import com.google.common.base.Preconditions;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted. It is expected that only a singleton instance of {@link QueryLimiter} will be created via CDI.
 */
public class QueryLimiter {

    private static final Logger log = Logger.getLogger(QueryLimiter.class);

    /**
     * A read-write lock that prevents queries from
     */
    private final ReadWriteLock limiterLock = new ReentrantReadWriteLock();

    /**
     * Whether this {@link QueryLimiter} is considered configured and activated.
     */
    private final AtomicBoolean activated = new AtomicBoolean(false);

    /**
     * A Zookeeper connect string or a path to a Zookeeper config file.
     */
    private String zookeeperConfig;

    /**
     * The query limits to enforce within this {@link QueryLimiter}.
     */
    private QueryLimitConfiguration configuration;

    /**
     * The timeout in milliseconds for {@link #client} to successfully connect to Zookeeper when {@link #activate()} is called. Defaults to 3 minutes.
     */
    private long zookeeperClientTimeoutMs = TimeUnit.MINUTES.toMillis(3);

    /**
     * The timeout in milliseconds for {@link #queryLogicCache} to reach an initial healthy state when {@link #activate()} is called. Defaults to 3 minutes.
     */
    private long queryLogicCacheTimeoutMs = TimeUnit.MINUTES.toMillis(3);

    /**
     * The timeout in milliseconds for {@link #queryCountsCache} to reach an initial healthy state when {@link #activate()} is called. Defaults to 3 minutes.
     */
    private long queryCountsCacheTimeoutMs = TimeUnit.MINUTES.toMillis(3);

    /**
     * The cache responsible for storing the heartbeats of active queries.
     */
    private QueryHeartbeatCache heartbeatCache;

    /**
     * The provider for query logic group limits.
     */
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;

    /**
     * The provider for user limits.
     */
    private UserLimitProvider userLimitProvider;

    /**
     * The provider for system limits.
     */
    private SystemLimitProvider systemLimitProvider;

    /**
     * The Zookeeper client.
     */
    private CuratorFramework client;

    /**
     * The tracker that handles tracking new active queries in Zookeeper.
     */
    private ActiveQueryTracker activeQueryTracker;

    /**
     * A local mirror cache of the distinct query logics stored in Zookeeper by {@link ActiveQueryTracker}.
     */
    private QueryLogicCache queryLogicCache;

    /**
     * A local mirror cache of the active queries stored in Zookeeper by {@link ActiveQueryTracker}.
     */
    private QueryCountsCache queryCountsCache;

    /**
     * Return the Zookeeper connection string or path to a Zookeeper config file.
     *
     * @return the Zookeeper configuration
     */
    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    /**
     * Set the Zookeeper configuration that is a connection string or path to a Zookeeper config file. Changes will not take effect until {@link #setup()} is
     * called.
     *
     * @param zookeeperConfig
     *            the zookeeper connection string
     */
    public void setZookeeperConfig(String zookeeperConfig) {
        limiterLock.writeLock().lock();
        try {
            this.zookeeperConfig = zookeeperConfig;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Set the configuration to use to set up this {@link QueryLimiter}. Changes will not take effect until {@link #setup()} is called.
     *
     * @param queryLimitConfiguration
     *            the config
     */
    public void setConfiguration(QueryLimitConfiguration queryLimitConfiguration) {
        limiterLock.writeLock().lock();
        try {
            this.configuration = queryLimitConfiguration;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Return the configuration used to set up this {@link QueryLimiter}.
     *
     * @return the config
     */
    public QueryLimitConfiguration getConfiguration() {
        limiterLock.readLock().lock();
        try {
            return configuration;
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Return the timeout in milliseconds for the Zookeeper client to successfully connect to Zookeeper during setup.
     *
     * @return the timeout
     */
    public long getZookeeperClientTimeoutMs() {
        limiterLock.readLock().lock();
        try {
            return zookeeperClientTimeoutMs;
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Set the timeout in milliseconds for the Zookeeper client to successfully connect to Zookeeper during setup.
     *
     * @param zookeeperClientTimeoutMs
     *            the timeout
     */
    public void setZookeeperClientTimeoutMs(long zookeeperClientTimeoutMs) {
        limiterLock.writeLock().lock();
        try {
            this.zookeeperClientTimeoutMs = zookeeperClientTimeoutMs;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Return the timeout in milliseconds for the query logic cache to reach a healthy state during setup.
     *
     * @return the timeout
     */
    public long getQueryLogicCacheTimeoutMs() {
        limiterLock.readLock().lock();
        try {
            return queryLogicCacheTimeoutMs;
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Set the timeout in milliseconds for the query logic cache to reach a healthy state during setup.
     *
     * @param queryLogicCacheTimeoutMs
     *            the timeout
     */
    public void setQueryLogicCacheTimeoutMs(long queryLogicCacheTimeoutMs) {
        limiterLock.writeLock().lock();
        try {
            this.queryLogicCacheTimeoutMs = queryLogicCacheTimeoutMs;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Return the timeout in milliseconds for the query count cache to reach a healthy state during setup.
     *
     * @return the timeout
     */
    public long getQueryCountsCacheTimeoutMs() {
        limiterLock.readLock().lock();
        try {
            return queryCountsCacheTimeoutMs;
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Set the timeout in milliseconds for the query count cache to reach a healthy state during setup.
     *
     * @param queryCountsCacheTimeoutMs
     *            the timeout
     */
    public void setQueryCountsCacheTimeoutMs(long queryCountsCacheTimeoutMs) {
        limiterLock.writeLock().lock();
        try {
            this.queryCountsCacheTimeoutMs = queryCountsCacheTimeoutMs;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Set the heartbeat cache to store query heartbeats in
     *
     * @param heartbeatCache
     *            the heartbeat cache
     */
    public void setHeartbeatCache(QueryHeartbeatCache heartbeatCache) {
        limiterLock.writeLock().lock();
        try {
            this.heartbeatCache = heartbeatCache;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Set up this {@link QueryLimiter} to become ready to enforce limits. In practice, this should be marked as the init method for the {@link QueryLimiter}
     * instance configured in bean configuration files.
     */
    public void setup() {
        if (log.isDebugEnabled()) {
            log.debug("Setting up query limiter.");
        }

        limiterLock.writeLock().lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Setting up with zookeeperConfig: '" + zookeeperConfig + "' and query limit config: " + configuration);
            }

            if (this.configuration != null) {
                activate();
            } else {
                clear();
            }

            if (log.isDebugEnabled()) {
                log.debug("Setup complete.");
            }
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Activate this {@link QueryLimiter}. The limiter will be deactivated if already active, and then internal caches, and limit providers will be
     * reinitialized based on the current configuration. NOTE: the Zookeeper client {@link #client} will be created only if it does not exist. Otherwise, the
     * pre-existing instance will be used.
     */
    private void activate() {
        // Exclusive lock. Blocks all calls that track queries/check for limits until the limiter is updated.
        limiterLock.writeLock().lock();
        try {
            if (this.configuration.getDefaultUserQueryLimit() < 1) {
                throw new IllegalArgumentException("Default user query limit must be greater than 0");
            }

            if (this.configuration.getInternalCacheMaxSize() < 1) {
                throw new IllegalArgumentException("Internal cache max size must be greater than 0");
            }

            if (this.heartbeatCache == null) {
                throw new IllegalStateException("No heartbeat cache set");
            }

            // Create the limit providers.
            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(configuration.getInternalCacheMaxSize(),
                            configuration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(configuration.getDefaultUserQueryLimit(), configuration.getInternalCacheMaxSize(),
                            configuration.getUserConfigs(), queryLogicGroupLimitProvider);
            this.systemLimitProvider = new SystemLimitProvider(configuration.getDefaultSystemQueryLimit(), configuration.getInternalCacheMaxSize(),
                            configuration.getSystemConfigs(), queryLogicGroupLimitProvider);

            // Create the Zookeeper client only if it is not already done so. In the case where the configuration for this QueryLimiter is later updated, we
            // want to preserve the pre-existing client so that we do not lose the ephemeral nodes maintained in the heartbeat cache.
            if (this.client == null) {
                // @formatter:off
                this.client = CuratorFrameworkFactory.builder()
                                .namespace(ZOOKEEPER_NAMESPACE)
                                .connectString(getQuorumPeerConfig(zookeeperConfig))
                                .sessionTimeoutMs(60_000)
                                .connectionTimeoutMs(60_000)
                                .retryPolicy(new RetryNTimes(10, 1000))
                                .build();
                // @formatter:on

                // Start the client and wait for it connect to Zookeeper.
                this.client.start();

                // @formatter:off
                try{
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for zookeeper client to connect with a timeout of " + zookeeperClientTimeoutMs + "ms");
                    }

                    Awaitility.await().alias("Zookeeper client connection")
                            .atMost(zookeeperClientTimeoutMs, TimeUnit.MILLISECONDS)
                            .untilAsserted(() -> this.client.blockUntilConnected());
                } catch (ConditionTimeoutException e){
                    log.warn("Zookeeper client failed to connect within timeout of " + zookeeperClientTimeoutMs + "ms");
                }
                // @formatter:on

                // Let the heartbeat cache listen for connection state changes.
                this.heartbeatCache.listenForConnectionStateChanges(this.client);
            }

            // The active query tracker instance can be reused between configuration updates.
            if (this.activeQueryTracker == null) {
                this.activeQueryTracker = new ActiveQueryTracker(client);
            }

            // The query logics cache instance can be reused between configuration updates.
            if (this.queryLogicCache == null) {
                // Create the query logic cache and wait for it to reach a healthy state.
                this.queryLogicCache = new QueryLogicCache(client);
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for query logic cache to reach healthy state with a timeout of " + queryLogicCacheTimeoutMs + "ms");
                    }

                    // @formatter:off
                    Awaitility.await().alias("Query logic cache initialization")
                            .atMost(queryLogicCacheTimeoutMs, TimeUnit.MILLISECONDS)
                            .until(() -> this.queryLogicCache.isHealthy());
                    // @formatter:on
                } catch (ConditionTimeoutException e) {
                    log.warn("Query logic cache failed to initialize within timeout of " + queryLogicCacheTimeoutMs + "ms");
                }

            }

            // Create the query counts cache if it doesn't exist and wait for it to reach a healthy state.
            if (this.queryCountsCache == null) {
                this.queryCountsCache = new QueryCountsCache(client, systemLimitProvider);
            } else {
                // If the query counts cache already exists, we need to update the system limit provider. This will trigger a rebuild of the internal map of
                // user query counts that may be impacted by changes to systems that count against user limits.
                if (log.isDebugEnabled()) {
                    log.debug("Query counts cache already exists. Updating system limits and rebuilding cache.");
                }
                this.queryCountsCache.setSystemLimitProvider(systemLimitProvider);
            }

            try {
                if (log.isDebugEnabled()) {
                    log.debug("Waiting for query counts cache to reach healthy state with a timeout of " + queryCountsCacheTimeoutMs + "ms");
                }

                // @formatter:off
                Awaitility.await().alias("Query counts cache initialization").atMost(queryCountsCacheTimeoutMs, TimeUnit.MILLISECONDS)
                                .until(() -> this.queryCountsCache.isHealthy());
                // @formatter:on
            } catch (ConditionTimeoutException e) {
                log.warn("Query counts cache failed to initialize within timeout of " + queryCountsCacheTimeoutMs + "ms");
            }

            // Update the query logic limit provider with the existing set of distinct query logics, and register a listener so that it will be notified of any
            // query logic additions/removals.
            this.queryLogicGroupLimitProvider.updateQueryLogics(this.queryLogicCache.getQueryLogics());
            this.queryLogicCache.addListener(this.queryLogicGroupLimitProvider.createQueryLogicsUpdateListener());

            this.activated.set(true);
        } catch (Exception e) {
            log.error("Activation failed. Deactivating query limiter.", e);
            clear();
            throw new QueryLimiterException("Activation failed.", e);
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Deactivate this {@link QueryLimiter}. Internal caches and limit providers will be closed and cleared. NOTE: The Zookeeper client {@link #client} and the
     * heartbeat cache {@link #heartbeatCache} are specifically NOT closed here. The Zookeeper client and heartbeat cache must be preserved and reused through
     * calls to this method and {@link #activate()} to preserve the existence of any ephemeral nodes maintained in the heartbeat cache. They should only be
     * cleaned up when {@link #shutdown()} is called.
     */
    private void clear() {
        // Exclusive lock. Blocks all calls that track queries/check for limits until the limiter is updated.
        limiterLock.writeLock().lock();
        try {
            this.activated.set(false);

            // Close the query logic cache.
            if (this.queryLogicCache != null) {
                try {
                    this.queryLogicCache.close();
                } catch (Exception e) {
                    log.warn("Error closing query logic cache", e);
                } finally {
                    this.queryLogicCache = null;
                }
            }

            // Close the query count cache.
            if (this.queryCountsCache != null) {
                try {
                    this.queryCountsCache.close();
                } catch (Exception e) {
                    log.warn("Error closing query counts cache", e);
                } finally {
                    this.queryCountsCache = null;
                }
            }

            // Clear the limit providers.
            this.queryLogicGroupLimitProvider = null;
            this.userLimitProvider = null;
            this.systemLimitProvider = null;
            this.activeQueryTracker = null;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Deactivate this {@link QueryLimiter} and close all connections and resources.
     */
    public void shutdown() {
        if (log.isDebugEnabled()) {
            log.debug("Shutting down.");
        }

        // Exclusive lock. Blocks all calls that track queries/check for limits until the limiter is updated.
        limiterLock.writeLock().lock();
        try {
            // Clear the query limiter.
            clear();

            // Close the Zookeeper client.
            if (this.client != null) {
                try {
                    this.client.close();
                } catch (Exception e) {
                    log.warn("Error closing zookeeper client", e);
                } finally {
                    this.client = null;
                }
            }
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Check if the user is allowed to create another query based on the given query logic on the current system. If the provided system is null or blank, a
     * default value of {@value QueryLimiterUtils#EMPTY_SYSTEM_FROM} will be used as the system.
     *
     * @param userDn
     *            the user DN
     * @param system
     *            the query system
     * @param queryLogic
     *            the query logic
     * @return the response
     * @throws Exception
     *             if an exception occurs
     */
    public QueryLimiterResponse checkForLimits(String userDn, String system, String queryLogic) throws Exception {
        Preconditions.checkArgument(userDn != null && !userDn.isBlank(), "userDn must not be blank");
        Preconditions.checkArgument(queryLogic != null && !queryLogic.isBlank(), "query logic must not be blank");

        // Use a read lock that allows for concurrent query tracking, but blocks if the query limiter configuration is being updated, and potentially the
        // caches are being rebuilt.
        limiterLock.readLock().lock();
        try {
            if (isEnforcingLimits()) {
                userDn = normalizeUserDn(userDn);
                queryLogic = normalizeQueryLogic(queryLogic);
                system = normalizeSystem(system);

                if (log.isDebugEnabled()) {
                    log.debug("Checking limits - userDn: " + userDn + ", system: " + system + ", queryLogic: " + queryLogic);
                }

                // Check if the snapshot reveals that any limits have been met.
                LimitChecker checker = new LimitChecker(userDn, system, queryLogic);
                checker.checkLimits();
                if (checker.metLimit) {
                    return QueryLimiterResponse.metLimit(checker.message);
                } else {
                    return QueryLimiterResponse.hasNotMetLimit();
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Query limits are not enforced.");
                }
                return QueryLimiterResponse.hasNotMetLimit();
            }
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Track the following information for the given query on Zookeeper for the current system, and count it towards any configured query limits. If the
     * provided system is null or blank, a default value of {@value QueryLimiterUtils#EMPTY_SYSTEM_FROM} will be used as the system. If this
     * {@link QueryLimiter} is not currently enforcing limits,
     *
     * @param queryId
     *            the query ID
     * @param userDn
     *            the userDN of the user who submitted the query
     * @param system
     *            the system from
     * @param queryLogic
     *            the queryLogic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    public void countQueryTowardsLimits(String queryId, String userDn, String system, String queryLogic) throws Exception {
        Preconditions.checkArgument(userDn != null && !userDn.isBlank(), "userDn must not be blank");
        Preconditions.checkArgument(queryLogic != null && !queryLogic.isBlank(), "query logic must not be blank");

        // Use a read lock that allows for concurrent query tracking, but blocks if the query limiter configuration is being updated, and potentially the
        // caches are being rebuilt.
        limiterLock.readLock().lock();
        try {
            if (isEnforcingLimits()) {
                if (log.isDebugEnabled()) {
                    log.debug("Start counting query " + queryId + " towards limits");
                }
                userDn = normalizeUserDn(userDn);
                system = normalizeSystem(system);
                queryLogic = normalizeQueryLogic(queryLogic);

                // Track the query in Zookeeper and store the provided heartbeat in the heartbeat cache. This acts as a means to keep the ephemeral node stored
                // in the heartbeat alive until explicitly closed.
                QueryHeartbeat heartbeat = this.activeQueryTracker.trackQuery(queryId, userDn, system, queryLogic);
                heartbeatCache.put(heartbeat);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Query limits are not enforced, query will not be tracked.");
                }
            }
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Return whether this {@link QueryLimiter} is currently enforcing limits, i.e. it has been initialized and the caches are trustworthy.
     *
     * @return true if this {@link QueryLimiter} is enforcing limits, or false otherwise
     */
    public boolean isEnforcingLimits() {
        limiterLock.readLock().lock();
        try {
            return activated.get() && queryLogicCache.isHealthy() && queryCountsCache.isHealthy();
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Fetch the set of query IDs for queries considered to be actively running by the this {@link QueryLimiter}.
     *
     * @return the set of IDs for active queries
     */
    public Set<String> getActiveQueries() {
        limiterLock.readLock().lock();
        try {
            return heartbeatCache != null ? heartbeatCache.getQueryIds() : Set.of();
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Clear the information for each of the given queries from Zookeeper, and stop counting them towards any configured query limits.
     *
     * @param queryIds
     *            the query IDs
     */
    public void stopCountingQueriesTowardsLimits(Set<String> queryIds) {
        if (log.isDebugEnabled()) {
            log.debug("Stopping counting queries towards limits: " + queryIds);
        }
        limiterLock.readLock().lock();
        try {
            if (heartbeatCache != null) {
                heartbeatCache.stopAndRemoveHeartbeats(queryIds);
            }
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Clear the information for the given query from Zookeeper, and stop counting it towards any configured query limits.
     *
     * @param queryId
     *            the query ID
     */
    public void stopCountingQueryTowardsLimits(String queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Stop counting query " + queryId + " towards limits");
        }
        limiterLock.readLock().lock();
        try {
            if (heartbeatCache != null) {
                heartbeatCache.stopAndRemoveHeartbeat(queryId);
            }
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Handles aggregating query totals and checking them against limits.
     */
    private class LimitChecker {

        private final String userDn;
        private final String system;
        private final String queryLogic;

        private boolean metLimit;
        private String message;

        public LimitChecker(String userDn, String system, String queryLogic) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogic = queryLogic;
        }

        /**
         * Check against any configured limits, and update whether a limit has been met.
         */
        public void checkLimits() {
            // Check against any configured user limits.
            checkUserLimits();
            if (metLimit) {
                return;
            }

            // If no limits were met, check against any system limits.
            checkSystemLimits();
        }

        /**
         * Check the limits configured for the user.
         */
        private void checkUserLimits() {
            // If there are custom limits configured for the user, check against them. Otherwise, check against the default user limits.
            if (userLimitProvider.hasCustomLimits(this.userDn)) {
                checkCustomUserLimits();
            } else {
                checkDefaultQueryLogicLimits();
            }
        }

        /**
         * Check against custom user limits.
         */
        private void checkCustomUserLimits() {
            UserLimits userLimits = userLimitProvider.getCustomLimits(this.userDn);
            Map<String,Integer> groupLimits;
            // If the user has custom query logic group limits, check query logic totals against them. Otherwise, check query logic totals against the default
            // query logic group limits.
            if (userLimits.overridesAnyGroupLimits()) {
                groupLimits = userLimits.getBestGroupLimits(this.queryLogic);
            } else {
                groupLimits = queryLogicGroupLimitProvider.getBestGroupLimits(this.queryLogic);
            }

            checkUserLimits(groupLimits, userLimits.getQueryLimit());
        }

        /**
         * Check against the default user limit and query logic group limits.
         */
        private void checkDefaultQueryLogicLimits() {
            Map<String,Integer> groupLimits = queryLogicGroupLimitProvider.getBestGroupLimits(this.queryLogic);
            checkUserLimits(groupLimits, userLimitProvider.getDefaultUserQueryLimit());
        }

        /**
         * Check if user has met the limit for either the target query logic or their max query limit.
         *
         * @param groupLimits
         *            the query logic group limits
         * @param queryLimit
         *            the max allowed queries
         */
        private void checkUserLimits(Map<String,Integer> groupLimits, int queryLimit) {
            // Check if the total user queries meets the limit.
            int totalUserQueries = queryCountsCache.getTotalUserQueries(this.userDn);
            if (totalUserQueries >= queryLimit) {
                this.metLimit = true;
                this.message = "User '" + this.userDn + "' has reached limit of " + queryLimit + " running queries";
                return;
            }

            // We're only interested in query logic group limits where the limit is less than or equal to the total user queries.
            // @formatter:off
            groupLimits = groupLimits.entrySet().stream()
                            .filter(e -> e.getValue() <= queryLimit)
                            .collect(Collectors.toMap(
                                            Map.Entry::getKey,
                                            Map.Entry::getValue,
                                            (oldValue, newValue) -> oldValue,
                                            LinkedHashMap::new));
            // @formatter:on

            // If we have any query logic group limits to check, do so.
            if (!groupLimits.isEmpty()) {
                // Create a set of limit checkers for each query logic group. This will only return limit checkers for groups where the group limit is equal to
                // or less than the total user queries. This set will be in order of lowest limit to highest.
                SortedSet<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);

                // Identify all query logics that fall within the target groups.
                Set<String> queryLogics = new HashSet<>();
                limitCheckers.forEach(limitChecker -> queryLogics.addAll(queryLogicGroupLimitProvider.getQueryLogicsForGroup(limitChecker.group)));

                // Fetch the total running queries for each query logic for the user.
                for (String queryLogic : queryLogics) {
                    int totalQueriesForQueryLogic = queryCountsCache.getTotalUserQueries(this.userDn, queryLogic);
                    // Update each group limit checker with the total. If we met a limit after doing so, update our status and return early.
                    for (QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                        limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                        if (limitChecker.limitMet()) {
                            this.metLimit = true;
                            this.message = "User '" + this.userDn + "' has reached limit of " + limitChecker.limit + " running queries for query logic group '"
                                            + limitChecker.group + "'";
                            return;
                        }
                    }
                }
            }
        }

        /**
         * Check if the system has met the limit for either the target query logic or their max query limit.
         */
        private void checkSystemLimits() {
            int queryLimit;
            Map<String,Integer> groupLimits = null;

            // Check if custom limits were configured for the system.
            Optional<SystemLimits> optional = systemLimitProvider.getCustomLimits(this.system);
            if (optional.isPresent()) {
                // If so, fetch the custom system query limit and any configured query logic group limits.
                SystemLimits systemLimits = optional.get();
                queryLimit = systemLimits.getQueryLimit();
                // Fetch any custom query logic group limits defined for the system for the query's logic.
                if (systemLimits.overridesAnyGroupLimits()) {
                    Map<String,Integer> bestGroupLimits = systemLimits.getBestGroupLimits(this.queryLogic);
                    if (!bestGroupLimits.isEmpty()) {
                        groupLimits = bestGroupLimits;
                    }
                }
            } else {
                queryLimit = systemLimitProvider.getDefaultSystemQueryLimit();
            }

            // If the system has a query limit, check if we've met it.
            if (queryLimit != QueryLimiterUtils.NO_LIMIT) {
                int totalSystemQueries = queryCountsCache.getTotalSystemQueries(this.system);
                // We've met the total system query limit. Update the status and return early.
                if (totalSystemQueries >= queryLimit) {
                    this.metLimit = true;
                    this.message = "System '" + this.system + "' has reached limit of " + queryLimit + " running queries";
                    return;
                } else {
                    // We're only interested in query logic group limits where the limit is less than or equal to the total system queries.
                    if (groupLimits != null) {
                        // @formatter:off
                        groupLimits = groupLimits.entrySet().stream()
                                        .filter(e -> e.getValue() <= queryLimit)
                                        .collect(Collectors.toMap(
                                                        Map.Entry::getKey,
                                                        Map.Entry::getValue,
                                                        (oldValue, newValue) -> oldValue,
                                                        LinkedHashMap::new));
                        // @formatter:on
                    }
                }
            }

            // If we have any query logic group limits, check them.
            if (groupLimits != null && !groupLimits.isEmpty()) {
                // Create a set of limit checkers for each query logic group. This set will be in order of lowest limit to highest.
                SortedSet<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);

                // Identify all query logics that fall within the target groups.
                Set<String> queryLogics = new HashSet<>();
                limitCheckers.forEach(limitChecker -> queryLogics.addAll(queryLogicGroupLimitProvider.getQueryLogicsForGroup(limitChecker.group)));

                // Fetch the total running queries for each query logic for the system.
                for (String queryLogic : queryLogics) {
                    int totalQueriesForQueryLogic = queryCountsCache.getTotalSystemQueries(this.system, queryLogic);
                    // Update each group limit checker with the total. If we met a limit after doing so, update our status and return early.
                    for (QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                        limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                        if (limitChecker.limitMet()) {
                            this.metLimit = true;
                            this.message = "System '" + this.system + "' has reached limit of " + limitChecker.limit
                                            + " running queries for query logic group '" + limitChecker.group + "'";
                            return;
                        }
                    }
                }
            }
        }

        /**
         * Return a set of {@link QueryLogicGroupLimitChecker} for the given groups to group limits map, in order of lowest limit to highest.
         *
         * @param groupsToLimits
         *            the groups to group limits map
         * @return a sorted set of limit checkers
         */
        private SortedSet<QueryLogicGroupLimitChecker> getQueryLogicLimitCheckers(Map<String,Integer> groupsToLimits) {
            SortedSet<QueryLogicGroupLimitChecker> checkers = new TreeSet<>();

            // If any relevant groups were found, include any other query logics that match against at least one of the relevant groups.
            // We track query logics that we have seen before (on this system and others) in Zookeeper.
            Set<String> groups = groupsToLimits.keySet();
            Map<String,Matcher> groupMatchers = queryLogicGroupLimitProvider.getGroupMatchers(groups);
            for (String group : groups) {
                checkers.add(new QueryLogicGroupLimitChecker(group, groupMatchers.get(group), groupsToLimits.get(group)));
            }

            return checkers;
        }
    }

    /**
     * Contains logic for making it easier to aggregate totals against a query logic group limit. If sorted, {@link QueryLogicGroupLimitChecker} will be sorted
     * by the limit in ascending order, and then the group name.
     */
    private static class QueryLogicGroupLimitChecker implements Comparable<QueryLogicGroupLimitChecker> {

        private final String group;
        private final Matcher matcher;
        private final int limit;
        private int total;

        private QueryLogicGroupLimitChecker(String group, Matcher matcher, int limit) {
            this.group = group;
            this.matcher = matcher;
            this.limit = limit;
        }

        public boolean matches(String queryLogic) {
            return matcher.matches(queryLogic);
        }

        public void incrementTotal(String queryLogic, int total) {
            if (matches(queryLogic)) {
                this.total = this.total + total;
            }
        }

        public boolean limitMet() {
            return total >= limit;
        }

        @Override
        public int compareTo(QueryLogicGroupLimitChecker other) {
            int result = Objects.compare(this.limit, other.limit, Comparator.naturalOrder());

            if (result == 0) {
                result = Objects.compare(this.group, other.group, Comparator.naturalOrder());
            }

            return result;
        }
    }
}
