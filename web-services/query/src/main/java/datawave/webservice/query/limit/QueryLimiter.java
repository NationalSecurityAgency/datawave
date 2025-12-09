package datawave.webservice.query.limit;

import java.util.HashMap;
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

    private String zookeeperConfig;
    private QueryLimitConfiguration configuration;

    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;
    private UserLimitProvider userLimitProvider;
    private SystemLimitProvider systemLimitProvider;

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

    /**
     * Return the {@link UserLimitProvider} instantiated after the last time {@link #setup()} was called.
     *
     * @return the user limit provider
     */
    UserLimitProvider getUserLimitProvider() {
        return userLimitProvider;
    }

    /**
     * Return the {@link SystemLimitProvider} instantiated after the last time {@link #setup()} was called.
     *
     * @return the system limit provider
     */
    SystemLimitProvider getSystemLimitProvider() {
        return systemLimitProvider;
    }

    /**
     * Return the {@link QueryLogicGroupLimitProvider} instantiated after the last time {@link #setup()} was called.
     *
     * @return the query logic group limit provider
     */
    QueryLogicGroupLimitProvider getQueryLogicGroupLimitProvider() {
        return queryLogicGroupLimitProvider;
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

            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(configuration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(configuration.getDefaultUserQueryLimit(), configuration.getUserConfigs(),
                            queryLogicGroupLimitProvider);
            this.systemLimitProvider = new SystemLimitProvider(configuration.getDefaultSystemQueryLimit(), configuration.getSystemConfigs(),
                            queryLogicGroupLimitProvider);
        } else {
            this.queryLogicGroupLimitProvider = null;
            this.userLimitProvider = null;
            this.systemLimitProvider = null;
        }
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

        if (log.isTraceEnabled()) {
            log.trace("Checking limits - userDn: " + userDn + ", system: " + system + ", queryLogic: " + queryLogic);
        }

        // Fetch the snapshot of actively running queries that are related to the user, system, and query logic.
        ActiveQuerySnapshot snapshot = getActiveQuerySnapshot(userDn, system, queryLogic);
        if (log.isTraceEnabled()) {
            log.trace("Checking limits - snapshot: " + snapshot);
        }

        // Check if the snapshot reveals that any limits have been met.
        LimitChecker checker = new LimitChecker(userDn, system, queryLogic, snapshot);
        checker.checkLimits();
        if (checker.metLimit) {
            return QueryLimiterResponse.metLimit(checker.message);
        } else {
            return QueryLimiterResponse.hasNotMetLimit();
        }
    }

    /**
     * Fetch a snapshot of active queries currently running that meet at least one of the following criteria:
     * <ul>
     * <li>Were submitted by the given user.</li>
     * <li>Were submitted on the given system.</li>
     * <li>Are based off the given queryLogic.</li>
     * </ul>
     *
     * @param userDn
     *            the userDn
     * @param system
     *            the system
     * @param queryLogic
     *            the queryLogic
     * @return the snapshot
     * @throws Exception
     *             if an error occurs
     */
    private ActiveQuerySnapshot getActiveQuerySnapshot(String userDn, String system, String queryLogic) throws Exception {
        ActiveQueryTracker tracker = getActiveQueryTracker();

        Set<String> recordedQueryLogics = tracker.getDistinctQueryLogics();
        Set<String> relevantQueryLogics = queryLogicGroupLimitProvider.getRelevantQueryLogics(queryLogic, recordedQueryLogics);

        return tracker.getSnapshot(userDn, system, relevantQueryLogics);
    }

    /**
     * Track the following information for the given query on Zookeeper for the current system. The system will be identified by the canonical hostname.
     *
     * @param queryId
     *            the queryId
     * @param userDn
     *            the userDN of the user who submitted the query
     * @param queryLogic
     *            the queryLogic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    public void trackQuery(String queryId, String userDn, String queryLogic) throws Exception {
        userDn = userDn.trim().toLowerCase();
        queryLogic = queryLogic.trim();

        getActiveQueryTracker().trackQuery(queryId, userDn, hostnameProvider.getCanonicalHostname(), queryLogic);
    }

    /**
     * Delete any information used for tracking the query status of the given query on Zookeeper.
     *
     * @param queryId
     *            the queryId
     * @throws Exception
     *             if an error occurs
     */
    public void stopTrackingQuery(String queryId) throws Exception {
        getActiveQueryTracker().stopTrackingQuery(queryId);
    }

    /**
     * Return a new heartbeat connected to Zookeeper for the given queryId.
     *
     * @param queryId
     *            the queryId
     * @return the heartbeat
     * @throws QuorumPeerConfig.ConfigException
     *             if an error occurs
     */
    public QueryHeartbeat createHeartbeat(String queryId) throws QuorumPeerConfig.ConfigException {
        return getActiveQueryTracker().createHeartbeat(queryId);
    }

    /**
     * Set the hostname provider. This only needs to be done for testing purposes when we want to avoid using {@link InetHostnameProvider}.
     *
     * @param provider
     *            the hostname provider
     */
    public void setHostnameProvider(HostnameProvider provider) {
        this.hostnameProvider = provider;
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
        private final ActiveQuerySnapshot snapshot;

        // Whether the user has a custom query logic group limit.
        private boolean userOverrodeQueryLogicLimit;

        // Whether a limit was met.
        private boolean metLimit;

        // The message to return.
        private String message;

        public LimitChecker(String userDn, String system, String queryLogic, ActiveQuerySnapshot snapshot) {
            this.userDn = userDn;
            this.system = system;
            this.queryLogic = queryLogic;
            this.snapshot = snapshot;
        }

        /**
         * Check the limits against the snapshot.
         */
        public void checkLimits() {
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
                Map<String,Integer> groupLimits = queryLogicGroupLimitProvider.getRelevantGroupLimits(queryLogic);
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
