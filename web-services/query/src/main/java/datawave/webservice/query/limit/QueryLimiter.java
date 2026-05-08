package datawave.webservice.query.limit;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import com.google.common.base.Preconditions;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted. It is expected that only a singleton instance of {@link QueryLimiter} will be created via CDI.
 */
public class QueryLimiter {

    private static final Logger log = Logger.getLogger(QueryLimiter.class);

    // The default string to use as a system name when no system is provided with a query.
    public static final String EMPTY_SYSTEM_FROM = "EMPTY_SYSTEM_FROM";

    // A lock that will guard access to the query limit configuration and the limit providers.
    private final Lock configLock = new ReentrantLock();

    // The string to use to connect to zookeeper.
    private String zookeeperConfig;

    // The configuration to initialize the limit providers with.
    private ImmutableQueryLimitConfiguration configuration;

    // A cache to store heartbeats of active queries within.
    private QueryHeartbeatCache heartbeatCache;

    // Provides configured limits for query logic groups.
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;

    // Provides configured limits for users.
    private UserLimitProvider userLimitProvider;

    // Provides configured limits for systems.
    private SystemLimitProvider systemLimitProvider;

    // The tracker responsible for interfacing with Zookeeper.
    private ActiveQueryTracker activeQueryTracker;

    // The config reloader responsible for notifying the query limiter when there are updates to the configuration.
    private QueryLimitConfigReloader configReloader;

    // Whether the limiter is currently in a state where it can provide limits
    private boolean canProvideLimits = false;

    /**
     * Return the zookeeper connection string.
     *
     * @return the zookeeper connection string
     */
    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    /**
     * Set the zookeeper connection string
     *
     * @param zookeeperConfig
     *            the zookeeper connection string
     */
    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    /**
     * Set the config reloader that will notify this {@link QueryLimiter} of configuration updates.
     *
     * @param configReloader
     *            the configuration reloader
     */
    public void setConfigReloader(QueryLimitConfigReloader configReloader) {
        this.configReloader = configReloader;
    }

    /**
     * Update the configuration for this {@link QueryLimiter}. The configuration will be validated if indicated, and the internal configuration and limit
     * providers will be recreated to reflect the new configuration.
     *
     * @param configuration
     *            the configuration to set
     * @param validationRequired
     *            whether the configuration should be validated before updating the internal providers
     */
    private void updateConfiguration(QueryLimitConfiguration configuration, boolean validationRequired) {
        Preconditions.checkNotNull(configuration, "configuration must not be null");

        configLock.lock();
        try {
            // If validation is required, do so.
            if (validationRequired) {
                QueryLimitConfigurationValidator.validate(configuration);
            }

            if (log.isDebugEnabled()) {
                log.debug("Updating configuration to " + configuration);
            }

            try {
                // Update the configuration.
                this.configuration = new ImmutableQueryLimitConfiguration(configuration);

                // Recreate the query logic group provider.
                if (this.queryLogicGroupLimitProvider != null) {
                    try {
                        this.queryLogicGroupLimitProvider.cleanUp();
                    } catch (Exception e) {
                        log.warn("Failed to clean up query logic group limit provider", e);
                    }
                    // Make this null so that if recreating the provider fails for some reason, canProvideLimits() will return false.
                    this.queryLogicGroupLimitProvider = null;
                }
                this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(configuration.getInternalCacheMaxSize(),
                                configuration.getQueryLogicGroupConfigs());

                // Recreate the user limit provider.
                if (this.userLimitProvider != null) {
                    try {
                        this.userLimitProvider.cleanUp();
                    } catch (Exception e) {
                        log.warn("Failed to clean up user limit provider", e);
                    }
                    // Make this null so that if recreating the provider fails for some reason, canProvideLimits() will return false.
                    this.userLimitProvider = null;
                }
                this.userLimitProvider = new UserLimitProvider(configuration.getDefaultUserQueryLimit(), configuration.getInternalCacheMaxSize(),
                                configuration.getUserConfigs(), queryLogicGroupLimitProvider);

                // Recreate the system limit provider.
                if (this.systemLimitProvider != null) {
                    try {
                        this.systemLimitProvider.cleanUp();
                    } catch (Exception e) {
                        log.warn("Failed to clean up system limit provider", e);
                    }
                    // Make this null so that if recreating the provider fails for some reason, canProvideLimits() will return false.
                    this.systemLimitProvider = null;
                }
                this.systemLimitProvider = new SystemLimitProvider(configuration.getDefaultSystemQueryLimit(), configuration.getInternalCacheMaxSize(),
                                configuration.getSystemConfigs(), queryLogicGroupLimitProvider);

                log.debug("Configuration updated and internal limit providers recreated");
            } catch (Exception e) {
                log.error("Failed to update configuration", e);
            }

            // Update whether this limiter can provide limits.
            this.canProvideLimits = this.configuration != null && this.queryLogicGroupLimitProvider != null && this.userLimitProvider != null
                            && this.systemLimitProvider != null;
        } finally {
            configLock.unlock();
        }
    }

    /**
     * Set the configuration for the {@link QueryLimiter} if and only if the configuration for the limiter is currently null. Throws an
     * {@link IllegalStateException} otherwise. This method exists primarily to support initial CDI injection during startup, and it is expected that
     * {@link #setup()} will be called to create the internal limit providers.
     *
     * @param queryLimitConfiguration
     *            the config
     *
     * @throws NullPointerException
     *             if the new configuration is null
     * @throws IllegalStateException
     *             if the internal {@link QueryLimitConfiguration} is not null.
     */
    public void setConfiguration(QueryLimitConfiguration queryLimitConfiguration) {
        Preconditions.checkNotNull(queryLimitConfiguration, "configuration must not be null");
        configLock.lock();
        try {
            if (this.configuration == null) {
                this.configuration = new ImmutableQueryLimitConfiguration(queryLimitConfiguration);
            } else {
                throw new IllegalStateException("QueryLimitConfiguration is already set, use updateConfiguration(QueryLimitConfiguration) instead");
            }
        } finally {
            configLock.unlock();
        }
    }

    /**
     * Return the configuration currently configured for this {@link QueryLimiter}. This will be an instance of {@link ImmutableQueryLimitConfiguration}.
     *
     * @return the config
     */
    public QueryLimitConfiguration getConfiguration() {
        configLock.lock();
        try {
            return configuration;
        } finally {
            configLock.unlock();
        }
    }

    /**
     * Set the {@link QueryHeartbeatCache}.
     *
     * @param heartbeatCache
     *            the heartbeat cache
     */
    public void setHeartbeatCache(QueryHeartbeatCache heartbeatCache) {
        this.heartbeatCache = heartbeatCache;
    }

    /**
     * Validate the configuration and extract the query limits to enforce. In practice this should be marked as the init method for the {@link QueryLimiter}
     * instance configured in bean XMLs. For testing purposes, this method should be called after setting the zookeeper config and query limit configs.
     */
    public void setup() {
        if (log.isDebugEnabled()) {
            log.debug("Initializing with zookeeperConfig: '" + zookeeperConfig + "', and query limit config: " + configuration);
        }

        configLock.lock();
        try {
            // Require the heartbeat cache to be set.
            if (heartbeatCache == null) {
                throw new IllegalStateException("No heartbeat cache set");
            }

            // If no configuration was supplied from a configured bean, attempt to load a configuration from Zookeeper.
            if (this.configuration == null) {
                if (this.configReloader != null) {
                    QueryLimitConfigReloader.LoadResult loadResult = configReloader.loadConfiguration();
                    if (loadResult.getStatus() == QueryLimitConfigReloader.ReloadStatus.SUCCESS) {
                        // Update the configuration and create the providers. The configuration returned by the reloader will already be validated.
                        updateConfiguration(loadResult.getConfig(), false);
                    }
                }
                if (this.configReloader == null) {
                    throw new IllegalStateException("No configuration supplied for Query Limiter via injection or Zookeeper.");
                }
            } else {
                // Update the configuration and create the providers.
                updateConfiguration(this.configuration, true);
            }

            // If the configuration reloader is not null, add a listener so that this limiter will be provided with new configurations. Any configs provided by
            // the reloader will already be validated.
            if (configReloader != null) {
                configReloader.addListener(((config) -> updateConfiguration(config, false)));
                log.debug("QueryLimiter now listening for configuration updates");
            } else {
                log.warn("No config reloader set for QueryLimiter, limiter will not be notified of configuration updates");
            }
        } finally {
            configLock.unlock();
        }
    }

    /**
     * Releases internal resources and cleans up connections and scheduled tasks.
     */
    public void shutdown() {
        log.debug("Shutting down");

        if (this.heartbeatCache != null) {
            try {
                this.heartbeatCache.shutdown();
            } catch (Exception e) {
                log.warn("Error closing heartbeat cache", e);
            } finally {
                this.heartbeatCache = null;
            }
        }
        if (this.activeQueryTracker != null) {
            try {
                this.activeQueryTracker.close();
            } catch (Exception e) {
                log.warn("Error closing active query tracker", e);
            } finally {
                this.activeQueryTracker = null;
            }
        }
        if (this.configReloader != null) {
            try {
                this.configReloader.close();
            } catch (Exception e) {
                log.warn("Error closing config reloader", e);
            } finally {
                this.configReloader = null;
            }
        }
    }

    /**
     * Check if the user is allowed to create another query based on the given query logic on the current system.
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
        configLock.lock();
        try {
            Preconditions.checkState(canProvideLimits, "Cannot check for limits, configuration or providers are not initialized");

            // Cast the user DN to lowercase to ensure a consistent format.
            userDn = userDn.trim().toLowerCase();

            // Do not cast the system or query logic to lowercase, they will be getting matched against regex patterns.
            queryLogic = queryLogic.trim();

            // Ensure the system is non-null if empty
            if (system == null || system.isBlank()) {
                system = EMPTY_SYSTEM_FROM;
            }

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
        } finally {
            configLock.unlock();
        }

    }

    /**
     * Track the following information for the given query on Zookeeper for the current system, and count it towards any configured query limits.
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
        configLock.lock();
        try {
            Preconditions.checkState(canProvideLimits, "Cannot check for limits, configuration or providers are not initialized");

            if (log.isDebugEnabled()) {
                log.debug("Start counting query " + queryId + " towards limits");
            }

            userDn = userDn.trim().toLowerCase();
            queryLogic = queryLogic.trim();
            // Ensure the system is non-null if empty
            if (system == null || system.isBlank()) {
                system = EMPTY_SYSTEM_FROM;
            }

            boolean systemCountsTowardsUserLimits = systemLimitProvider.countsAgainstUserLimit(system);

            QueryHeartbeat heartbeat = getActiveQueryTracker().trackQuery(queryId, userDn, system, queryLogic, systemCountsTowardsUserLimits);
            // Store the heartbeat into the cache. This acts as a means to keep the connection to Zookeeper alive for the ephemeral nodes stored in the
            // heartbeat.
            heartbeatCache.put(heartbeat);
        } finally {
            configLock.unlock();
        }
    }

    /**
     * Fetch the set of query IDs for queries considered to be actively running by the this {@link QueryLimiter}.
     *
     * @return the set of IDs for active queries
     */
    public Set<String> getActiveQueries() {
        return heartbeatCache.getQueryIds();
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
        heartbeatCache.stopAndRemoveHeartbeats(queryIds);
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
        heartbeatCache.stopAndRemoveHeartbeat(queryId);
    }

    /**
     * Return the {@link ActiveQueryTracker} instance, initializing it if needed.
     *
     * @return the active query tracker
     */
    private ActiveQueryTracker getActiveQueryTracker() throws QuorumPeerConfig.ConfigException {
        if (this.activeQueryTracker == null) {
            this.activeQueryTracker = new ActiveQueryTracker(zookeeperConfig, 120000L);
        }
        return this.activeQueryTracker;
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
        private List<String> distinctQueryLogics;

        public LimitChecker(String userDn, String system, String queryLogic) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogic = queryLogic;
        }

        /**
         * Check against any configured limits, and update whether a limit has been met.
         */
        public void checkLimits() throws Exception {
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
        private void checkUserLimits() throws Exception {
            // If there are custom limits configured for the user, check against them. Otherwise, check against the default user limits.
            if (userLimitProvider.hasCustomLimits(userDn)) {
                checkCustomUserLimits();
            } else {
                checkDefaultQueryLogicLimits();
            }
        }

        /**
         * Check against custom user limits.
         */
        private void checkCustomUserLimits() throws Exception {
            UserLimits userLimits = userLimitProvider.getCustomLimits(userDn);
            Map<String,Integer> groupLimits;
            // If the user has custom query logic group limits, check query logic totals against them. Otherwise, check query logic totals against the default
            // query logic group limits.
            if (userLimits.overridesAnyGroupLimits()) {
                groupLimits = userLimits.getBestGroupLimits(queryLogic);
            } else {
                groupLimits = queryLogicGroupLimitProvider.getBestGroupLimits(queryLogic);
            }

            checkUserLimits(groupLimits, userLimits.getQueryLimit());
        }

        /**
         * Check against the default user limit and query logic group limits.
         */
        private void checkDefaultQueryLogicLimits() throws Exception {
            Map<String,Integer> groupLimits = queryLogicGroupLimitProvider.getBestGroupLimits(queryLogic);
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
        private void checkUserLimits(Map<String,Integer> groupLimits, int queryLimit) throws Exception {
            Set<String> queryLogics = new HashSet<>();
            int totalUserQueries = 0;
            ActiveQueryTracker tracker = getActiveQueryTracker();

            // If groupsLimit is not empty, then we found one or more best-matching groups for the query logic.
            if (!groupLimits.isEmpty()) {
                // Create a set of limit checkers for each query logic group.
                Set<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);

                // Load the distinct query logics, and fetch all query logics that fall within the target groups.
                loadDistinctQueryLogics();
                limitCheckers.forEach(limitChecker -> queryLogics.addAll(limitChecker.matcher.getMatches(distinctQueryLogics)));

                // Fetch the total running queries for each query logic for the user.
                for (String queryLogic : queryLogics) {
                    int totalQueriesForQueryLogic = tracker.getTotalUserQueriesForQueryLogic(userDn, queryLogic);
                    // Update each group limit checker with the total. If we met a limit after doing so, update our status and return early.
                    for (QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                        limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                        if (limitChecker.limitMet()) {
                            this.metLimit = true;
                            this.message = "User '" + userDn + "' has reached limit of " + limitChecker.limit + " running queries for query logic group '"
                                            + limitChecker.group + "'";
                            return;
                        }
                    }

                    // Update the current total user queries, and check if we've met a limit. If so, update our status the return early.
                    totalUserQueries += totalQueriesForQueryLogic;
                    if (totalUserQueries >= queryLimit) {
                        this.metLimit = true;
                        this.message = "User '" + userDn + "' has reached limit of " + queryLimit + " running queries";
                        return;
                    }
                }
            }

            // If we've reached this point, we did not meet any user limits configured for the query logic. Check if the total number of queries for the user
            // meets their max query limit. Pass in the query logics we already counted as well as the current total to avoid unnecessary scanning in Zookeeper.
            if (tracker.totalUserQueriesMeetsLimit(userDn, queryLimit, queryLogics, totalUserQueries)) {
                this.metLimit = true;
                this.message = "User '" + userDn + "' has reached limit of " + queryLimit + " running queries";
            }
        }

        /**
         * Check if the system has met the limit for either the target query logic or their max query limit.
         */
        private void checkSystemLimits() throws Exception {
            Set<String> queryLogics = new HashSet<>();
            int totalSystemQueries = 0;
            int queryLimit = systemLimitProvider.getDefaultSystemQueryLimit();
            ActiveQueryTracker tracker = getActiveQueryTracker();

            // Check if any custom limits apply for the system.
            Optional<SystemLimits> optional = systemLimitProvider.getCustomLimits(system);
            if (optional.isPresent()) {
                SystemLimits systemLimits = optional.get();
                // If so, update the system query limit to use the custom value.
                queryLimit = systemLimits.getQueryLimit();

                // If the system has any custom query logic group limits, check if the query logic applies to any of the groups.
                if (systemLimits.overridesAnyGroupLimits()) {
                    Map<String,Integer> groupLimits = systemLimits.getBestGroupLimits(queryLogic);
                    // If groupsLimit is not empty, then we found one or more best-matching groups for the query logic.
                    if (!groupLimits.isEmpty()) {
                        // Create a set of limit checkers for each query logic group.
                        Set<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);

                        // Load the distinct query logics, and fetch all query logics that fall within the target groups.
                        loadDistinctQueryLogics();
                        limitCheckers.forEach(limitChecker -> queryLogics.addAll(limitChecker.matcher.getMatches(distinctQueryLogics)));

                        // Fetch the total running queries for each query logic for the system.
                        for (String queryLogic : queryLogics) {
                            int totalQueriesForQueryLogic = tracker.getTotalSystemQueriesForQueryLogic(system, queryLogic);
                            // Update each group limit checker with the total. If we met a limit after doing so, update our status and return early.
                            for (QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                                limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                                if (limitChecker.limitMet()) {
                                    this.metLimit = true;
                                    this.message = "System '" + system + "' has reached limit of " + limitChecker.limit
                                                    + " running queries for query logic group '" + limitChecker.group + "'";
                                    return;
                                }
                            }

                            // If the system has a query limit, check if we've reached it.
                            if (queryLimit != QueryLimitConstants.NO_LIMIT) {
                                // Update the current total system queries, and check if we've met a limit. If so, update our status the return early.
                                totalSystemQueries += totalQueriesForQueryLogic;
                                if (totalSystemQueries >= queryLimit) {
                                    this.metLimit = true;
                                    this.message = "System '" + system + "' has reached limit of " + queryLimit + " running queries";
                                    return;
                                }
                            }
                        }
                    }
                }
            }

            // If we've reached this point, we did not meet any system limits configured for the query logic. Check if the total number of queries for the
            // system meets their max query limit. Pass in the query logics we already counted as well as the current total to avoid unnecessary scanning in
            // Zookeeper.
            if (queryLimit != QueryLimitConstants.NO_LIMIT) {
                if (tracker.totalSystemQueriesMeetsLimit(system, queryLimit, queryLogics, totalSystemQueries)) {
                    this.metLimit = true;
                    this.message = "System '" + system + "' has reached limit of " + queryLimit + " running queries";
                }
            }
        }

        private Set<QueryLogicGroupLimitChecker> getQueryLogicLimitCheckers(Map<String,Integer> groupsToLimits) {
            Set<QueryLogicGroupLimitChecker> checkers = new HashSet<>();

            // If any relevant groups were found, include any other query logics that match against at least one of the relevant groups.
            // We track query logics that we have seen before (on this system and others) in Zookeeper.
            Set<String> groups = groupsToLimits.keySet();
            Map<String,Matcher> groupMatchers = queryLogicGroupLimitProvider.getGroupMatchers(groups);
            for (String group : groups) {
                checkers.add(new QueryLogicGroupLimitChecker(group, groupMatchers.get(group), groupsToLimits.get(group)));
            }

            return checkers;
        }

        /**
         * Load the set of distinct query logics from Zookeeper if not yet loaded.
         */
        private void loadDistinctQueryLogics() throws QuorumPeerConfig.ConfigException {
            if (distinctQueryLogics == null) {
                distinctQueryLogics = getActiveQueryTracker().getDistinctQueryLogics();
            }
        }
    }

    /**
     * Contains logic for making it easier to aggregate totals against a query logic group limit.
     */
    private static class QueryLogicGroupLimitChecker {

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

    }
}
