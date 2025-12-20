package datawave.webservice.query.limit;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted.
 */
public class QueryLimiter {

    private static final Logger log = Logger.getLogger(QueryLimiter.class);

    // Default to using InetAddress
    private HostnameProvider hostnameProvider = HostnameProvider.getInetAddressProvider();

    // The string to use to connect to zookeeper.
    private String zookeeperConfig;

    // The configuration to initialize the limit providers with.
    private QueryLimitConfiguration configuration;

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
     * Set the configuration to use to set up this {@link QueryLimiter}
     *
     * @param queryLimitConfiguration
     *            the config
     */
    public void setConfiguration(QueryLimitConfiguration queryLimitConfiguration) {
        this.configuration = queryLimitConfiguration;
    }

    /**
     * Return the configuration used to set up this {@link QueryLimiter}
     *
     * @return the config
     */
    public QueryLimitConfiguration getConfiguration() {
        return configuration;
    }

    public void setHeartbeatCache(QueryHeartbeatCache heartbeatCache) {
        this.heartbeatCache = heartbeatCache;
    }

    /**
     * Validate the configuration and extract the query limits to enforce. In practice this should be marked as the init method for the {@link QueryLimiter}
     * instance configured in bean XMLs. For testing purposes, this method should be called after setting the zookeeper config and query limit configs.
     */
    public void setup() {
        if (log.isDebugEnabled()) {
            log.debug("Initializing with zookeeperConfig: '" + zookeeperConfig + "' and query limit config: " + configuration);
        }

        if (this.configuration != null) {
            if (this.configuration.getDefaultUserQueryLimit() < 1) {
                throw new IllegalArgumentException("Default user query limit must be greater than 0");
            }

            if (this.configuration.getDefaultSystemQueryLimit() < 1) {
                throw new IllegalArgumentException("Default system query limit must be greater than 0");
            }

            if (this.configuration.getInternalCacheMaxSize() < 1) {
                throw new IllegalArgumentException("Internal cache max size must be greater than 0");
            }

            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(configuration.getInternalCacheMaxSize(),
                            configuration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(configuration.getDefaultUserQueryLimit(), configuration.getInternalCacheMaxSize(),
                            configuration.getUserConfigs(), queryLogicGroupLimitProvider);
            this.systemLimitProvider = new SystemLimitProvider(configuration.getDefaultSystemQueryLimit(), configuration.getInternalCacheMaxSize(),
                            configuration.getSystemConfigs(), queryLogicGroupLimitProvider);
        } else {
            this.queryLogicGroupLimitProvider = null;
            this.userLimitProvider = null;
            this.systemLimitProvider = null;
        }
    }

    /**
     * Releases internal resources and cleans up connections and scheduled tasks.
     */
    public void shutdown() {
        if (this.heartbeatCache != null) {
            try {
                this.heartbeatCache.shutdown();
            } catch (Exception e) {
                log.error("Error closing heartbeat cache", e);
            }
        }
        if (this.activeQueryTracker != null) {
            try {
                this.activeQueryTracker.close();
            } catch (Exception e) {
                log.error("Error closing active query tracker", e);
            }
        }
    }

    /**
     * Set the hostname provider to use for this {@link QueryLimiter}. Provided for testing purposes.
     *
     * @param hostnameProvider
     *            the hostname provider
     */
    public void setHostnameProvider(HostnameProvider hostnameProvider) {
        this.hostnameProvider = hostnameProvider;
    }

    /**
     * Check if the user is allowed to create another query based on the given query logic on the current system.
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @return the response
     * @throws Exception
     *             if an exception occurs
     */
    public QueryLimiterResponse checkForLimits(String userDn, String queryLogic) throws Exception {
        // Cast the user DN to lowercase to ensure a consistent format.
        userDn = userDn.trim().toLowerCase();

        // Do not cast the system or query logic to lowercase, they will be getting matched against regex patterns.
        String system = hostnameProvider.getCanonicalHostname();
        queryLogic = queryLogic.trim();

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
    }

    /**
     * Track the following information for the given query on Zookeeper for the current system, and count it towards any configured query limits. The system
     * will be identified by the canonical hostname.
     *
     * @param queryId
     *            the query ID
     * @param userDn
     *            the userDN of the user who submitted the query
     * @param queryLogic
     *            the queryLogic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    public void countQueryTowardsLimits(String queryId, String userDn, String queryLogic) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Start counting query " + queryId + " towards limits");
        }

        userDn = userDn.trim().toLowerCase();
        queryLogic = queryLogic.trim();

        QueryHeartbeat heartbeat = getActiveQueryTracker().trackQuery(queryId, userDn, hostnameProvider.getCanonicalHostname(), queryLogic);
        // Store the heartbeat into the cache. This acts as a means to keep the connection to Zookeeper alive for the ephemeral nodes stored in the heartbeat.
        heartbeatCache.put(heartbeat);
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
     * Evaluates a {@link ActiveQuerySnapshot} to see if it meets any limits.
     */
    private class LimitChecker {

        private final String userDn;
        private final String system;
        private final String queryLogic;
        private ActiveQuerySnapshot snapshot;

        // Whether the user has a custom query logic group limit.
        private boolean userOverrodeQueryLogicLimit;

        // Whether a limit was met.
        private boolean metLimit;

        // The message to return.
        private String message;

        public LimitChecker(String userDn, String system, String queryLogic) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogic = queryLogic;
        }

        /**
         * Check the limits against the snapshot.
         */
        public void checkLimits() throws Exception {
            loadSnapshot();

            // Check limits configured for the user.
            checkUserLimits();
            if (metLimit) {
                return;
            }

            // Check limits configured for the system.
            checkSystemLimits();
            if (metLimit) {
                return;
            }

            // Check limits configured for the query logic.
            checkQueryLogicLimits();
        }

        /**
         * Load the snapshot of relevant active queries.
         *
         * @throws Exception
         *             if an error occurs while fetching the snapshot.
         */
        private void loadSnapshot() throws Exception {
            // When fetching the snapshot of query activity, we need to include queries for any query logics that may count towards a query logic group limit
            // applicable for the candidate user, system, and query logic. The applicable query logic groups depend on the default query logic group limits,
            // along with any custom limits defined for the user or system.
            Set<String> queryLogics = getQueryLogicsToFetchInSnapshot();
            this.snapshot = getActiveQueryTracker().getSnapshot(userDn, system, queryLogics);
            if (log.isTraceEnabled()) {
                log.trace("Loaded snapshot: " + snapshot);
            }
        }

        /**
         * Return the set of query logics to filter on when loading the snapshot.
         */
        private Set<String> getQueryLogicsToFetchInSnapshot() throws QuorumPeerConfig.ConfigException {
            Set<String> groups = new HashSet<>();

            // If the user has custom group limits, find the best matching groups for the query logic, and include them. The best-matching groups may differ
            // between the default limits and custom user limits, so check the custom limits first.
            boolean userOverrodeGroupLimits = false;
            if (userLimitProvider.hasCustomLimits(userDn)) {
                UserLimits userLimits = userLimitProvider.getCustomLimits(userDn);
                if (userLimits.overridesAnyGroupLimits()) {
                    userOverrodeGroupLimits = true;
                    Map<String,Integer> groupLimits = userLimits.getRelevantGroupLimits(queryLogic);
                    groups.addAll(groupLimits.keySet());
                }
            }

            // If there were no custom group limits for the user, then check against the default group limits.
            if (!userOverrodeGroupLimits) {
                Map<String,Integer> groupLimits = queryLogicGroupLimitProvider.getGroupLimits(queryLogic);
                groups.addAll(groupLimits.keySet());
            }

            // If the system has custom limits, include any groups they override that match against the query logic.
            Optional<SystemLimits> systemLimits = systemLimitProvider.getCustomLimits(system);
            if (systemLimits.isPresent()) {
                SystemLimits systemLimit = systemLimits.get();
                if (systemLimit.overridesAnyGroupLimits()) {
                    Map<String,Integer> groupLimits = systemLimit.getRelevantGroupLimits(queryLogic);
                    groups.addAll(groupLimits.keySet());
                }
            }

            // If we found no relevant groups, return a set consisting of the query logics.
            Set<String> queryLogics = new HashSet<>();
            // Ensure we always at least return the original query logic.
            queryLogics.add(queryLogic);
            // If any relevant groups were found, include any other query logics that match against at least one of the relevant groups.
            if (!groups.isEmpty()) {
                // We track query logics that we have seen before (on this system and others) in Zookeeper.
                Set<String> distinctQueryLogics = getActiveQueryTracker().getDistinctQueryLogics();
                Map<String,Matcher> groupMatchers = queryLogicGroupLimitProvider.getGroupMatchers(groups);
                for (String group : groups) {
                    // Include any query logics that match against a group.
                    Matcher matcher = groupMatchers.get(group);
                    queryLogics.addAll(matcher.getMatches(distinctQueryLogics));
                }
            }

            return queryLogics;
        }

        /**
         * Check if the user has met any limits.
         */
        private void checkUserLimits() {
            // If custom limits were configured for the user, check them.
            if (userLimitProvider.hasCustomLimits(userDn)) {
                checkCustomUserLimits();
            } else {
                // Otherwise, check if the user has met the default maximum number of concurrent queries for users.
                checkUserQueryLimits(userLimitProvider.getDefaultUserQueryLimit());
            }
        }

        /**
         * Check if any custom limits for the user were met.
         */
        private void checkCustomUserLimits() {
            UserLimits customLimits = userLimitProvider.getCustomLimits(userDn);

            // Check if the user has met their query limit.
            checkUserQueryLimits(customLimits.getQueryLimit());
            // Return early if we've met a limit.
            if (this.metLimit) {
                return;
            }

            // Check if the user has met a limit for a query logic group.
            if (customLimits.overridesAnyGroupLimits()) {
                // Mark that we've checked query logic group limits so that we don't check again with the default limits for query logic groups.
                this.userOverrodeQueryLogicLimit = true;
                // Check if another query in the query logic would exceed any query logic group limits for the user.
                // It is possible for the query logic to match against one of the following:
                // - A single exact match for a group.
                // - A single wildcard match for a group.
                // - Multiple partial regex matches for a group.
                // The map of groups to limit will always be sorted in with the lowest limit first. In the case of multiple partial regex matches, we must check
                // against the limit for each group in case we meet the limit for one that is more inclusive than the other.
                Map<String,Integer> groupLimits = customLimits.getRelevantGroupLimits(queryLogic);
                checkUserQueryLogicLimits(groupLimits);
            }
        }

        /**
         * Check if another query by the user would exceed the given limit.
         *
         * @param limit
         *            the limit to respect
         */
        private void checkUserQueryLimits(int limit) {
            // When counting queries by the user, we must only count queries running on systems that count against the user query limit.
            int totalQueries = getTotalUserQueriesThatCountAgainstLimit();
            if (totalQueries >= limit) {
                this.message = "User '" + userDn + "' has reached limit of " + limit + " running queries";
                this.metLimit = true;
            }
        }

        /**
         * Return the total queries the user has running that count towards their query limit.
         */
        private int getTotalUserQueriesThatCountAgainstLimit() {
            // The total number of queries a user has running, and the total number of those queries that count towards the user's query limit may differ. It
            // depends on the systems the queries were submitted on, and whether queries on the systems count towards the user's query limit. Evaluate each
            // system
            // that the user has queries on, and only sum up the queries on systems that count towards the user's query limit.
            int totalQueries = 0;
            Map<String,Integer> totalUserQueriesPerSystem = snapshot.getUserQueriesPerSystem();
            for (Map.Entry<String,Integer> entry : totalUserQueriesPerSystem.entrySet()) {
                String system = entry.getKey();
                if (systemLimitProvider.countsAgainstUserLimit(system)) {
                    totalQueries += entry.getValue();
                }
            }
            return totalQueries;
        }

        /**
         * Check the system has met any limits.
         */
        private void checkSystemLimits() {
            // Fetch the best-matching custom system limit for the system.
            Optional<SystemLimits> customLimits = systemLimitProvider.getCustomLimits(system);
            // If we found a custom limit, check the limits defined therein.
            if (customLimits.isPresent()) {
                checkCustomSystemLimits(customLimits.get());
            } else {
                // If no custom limits were defined for the system, then at least check if the system has met the default number of running queries.
                checkSystemQueryLimits(systemLimitProvider.getDefaultSystemQueryLimit());
            }
        }

        /**
         * Check if any custom limits for the system were met.
         *
         * @param systemLimits
         *            the system limits
         */
        private void checkCustomSystemLimits(SystemLimits systemLimits) {
            // Check if the system has reached its overall query limit.
            checkSystemQueryLimits(systemLimits.getQueryLimit());
            // If so, return early.
            if (this.metLimit) {
                return;
            }

            // Check if the system has reached a limit for a query logic group.
            if (systemLimits.overridesAnyGroupLimits()) {
                // It is possible for the query logic to match against one of the following:
                // - A single exact match for a group.
                // - A single wildcard match for a group.
                // - Multiple partial regex matches for a group.
                // The map of groups to limit will always be sorted in with the lowest limit first. In the case of multiple partial regex matches, we must check
                // against the limit for each group in case we meet the limit for one that is more inclusive than the other.
                Map<String,Integer> groupLimits = systemLimits.getRelevantGroupLimits(queryLogic);
                if (!groupLimits.isEmpty()) {
                    Map<String,Integer> groupCounts = getTotalSystemQueriesForGroups(groupLimits.keySet());
                    for (String group : groupLimits.keySet()) {
                        int limit = groupLimits.get(group);
                        int total = groupCounts.get(group);
                        if (total >= limit) {
                            this.message = "System '" + system + "' has reached limit of " + limit + " running queries for query logic group '" + group + "'";
                            this.metLimit = true;
                            return;
                        }
                    }
                }
            }
        }

        private Map<String,Integer> getTotalSystemQueriesForGroups(Set<String> groups) {
            Map<String,Matcher> groupMatchers = queryLogicGroupLimitProvider.getGroupMatchers(groups);
            Map<String,Integer> groupCounts = new HashMap<>();
            Map<String,Integer> queryLogicCounts = snapshot.getTotalSystemQueriesPerQueryLogic();
            countGroupQueries(groupMatchers, groupCounts, queryLogicCounts);
            return groupCounts;
        }

        /**
         * Check if another query on the system would exceed the given limit.
         *
         * @param limit
         *            the limit to respect
         */
        private void checkSystemQueryLimits(int limit) {
            int totalQueries = snapshot.getTotalSystemQueries();
            if (totalQueries >= limit) {
                this.message = "System '" + system + "' has reached limit of " + limit + " running queries";
                this.metLimit = true;
            }
        }

        /**
         * Check if another query by the user for the query logic group would exceed any default query logic group limits.
         */
        private void checkQueryLogicLimits() {
            // Only perform this check if we did not previously have custom group limits defined for the user.
            if (!userOverrodeQueryLogicLimit) {
                Map<String,Integer> groupLimits = queryLogicGroupLimitProvider.getGroupLimits(queryLogic);
                checkUserQueryLogicLimits(groupLimits);
            }
        }

        /**
         * Check if another query by the user for the query logic would exceed any of the given limits for the given groups.
         *
         * @param groupLimits
         *            the group limits
         */
        private void checkUserQueryLogicLimits(Map<String,Integer> groupLimits) {
            if (!groupLimits.isEmpty()) {
                // Get a map of groups to the total number of actively running queries based on query logics that match against the group.
                Map<String,Integer> groupCounts = getTotalUserQueriesForGroupsThatCountAgainstLimit(groupLimits.keySet());
                // Check if any limits were exceeded.
                for (String group : groupLimits.keySet()) {
                    int limit = groupLimits.get(group);
                    int total = groupCounts.getOrDefault(group, 0);
                    if (total >= limit) {
                        this.message = "User '" + userDn + "' has reached limit of " + limit + " running queries for query logic group '" + group + "'";
                        this.metLimit = true;
                        return;
                    }
                }
            }
        }

        private Map<String,Integer> getTotalUserQueriesForGroupsThatCountAgainstLimit(Set<String> groups) {
            Map<String,Matcher> groupMatchers = queryLogicGroupLimitProvider.getGroupMatchers(groups);
            Map<String,Integer> groupCounts = new HashMap<>();
            Map<String,Map<String,Integer>> totalUserQueriesPerSystemPerQueryLogic = snapshot.getTotalUserQueriesPerSystemPerQueryLogic();

            for (String system : totalUserQueriesPerSystemPerQueryLogic.keySet()) {
                // We only want to count queries on systems that count against the user limits.
                if (systemLimitProvider.countsAgainstUserLimit(system)) {
                    Map<String,Integer> queryLogicCounts = totalUserQueriesPerSystemPerQueryLogic.get(system);
                    countGroupQueries(groupMatchers, groupCounts, queryLogicCounts);
                }
            }
            return groupCounts;
        }

        private void countGroupQueries(Map<String,Matcher> groupMatchers, Map<String,Integer> groupCounts, Map<String,Integer> queryLogicCounts) {
            for (String queryLogic : queryLogicCounts.keySet()) {
                for (String group : groupMatchers.keySet()) {
                    Matcher matcher = groupMatchers.get(group);
                    if (matcher.matches(queryLogic)) {
                        int queryLogicCount = queryLogicCounts.get(queryLogic);
                        groupCounts.compute(group, (k, v) -> v == null ? queryLogicCount : v + queryLogicCount);
                    }
                }
            }
        }
    }
}
