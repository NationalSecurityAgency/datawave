package datawave.webservice.query.limit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted.
 */
public class QueryLimiter {

    private static final Logger log = Logger.getLogger(QueryLimiter.class);

    // The server name used should be the hostname.
    private static final String HOSTNAME;

    static {
        try {
            HOSTNAME = InetAddress.getLocalHost().getCanonicalHostName();
            if(log.isDebugEnabled()) {
                log.debug("Set server hostname to '" + HOSTNAME + "'");
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException("Failed to fetch hostname", e);
        }
    }

    private String zookeeperConfig;
    private QueryLimitConfiguration configuration;

    private UserLimitProvider userLimitProvider;
    private SystemLimitProvider systemLimitProvider;
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;

    private ActiveQueryTracker activeQueryTracker;

    public String getZookeeperConfig() {
        return zookeeperConfig;
    }

    public void setZookeeperConfig(String zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
    }

    public void setConfiguration(QueryLimitConfiguration queryLimitConfiguration) {
        this.configuration = queryLimitConfiguration;
    }

    public QueryLimitConfiguration getConfiguration() {
        return configuration;
    }

    public UserLimitProvider getUserLimitProvider() {
        return userLimitProvider;
    }

    public SystemLimitProvider getSystemLimitProvider() {
        return systemLimitProvider;
    }

    public QueryLogicGroupLimitProvider getQueryLogicGroupLimitProvider() {
        return queryLogicGroupLimitProvider;
    }

    /**
     * Validate the configuration and extract the query limits to enforce.
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

            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(this.configuration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(this.configuration.getDefaultUserQueryLimit(),
                            this.configuration.getUserConfigs());
            this.systemLimitProvider = new SystemLimitProvider(this.configuration.getDefaultSystemQueryLimit(),
                            this.configuration.getSystemConfigs());
        } else {
            this.userLimitProvider = null;
            this.systemLimitProvider = null;
            this.queryLogicGroupLimitProvider = null;
        }
    }

    /**
     * Return whether submitting a new query for the given user, based on the given query logic, would exceed a configured concurrent query limit for the
     * current system. The current system will be identified by the canonical hostname.
     *
     * @param userDn
     *            the user DN
     * @param queryLogic
     *            the query logic
     * @return the response
     */
    public QueryLimiterResponse checkForLimits(String userDn, String queryLogic) throws Exception {
        String hostname = InetAddress.getLocalHost().getCanonicalHostName();
        return checkForLimits(userDn, hostname, queryLogic);
    }

    /**
     * Return whether submitting a new query for the given user, on the given system, based on the given query logic, would exceed a configured concurrent query
     * limit.
     *
     * @param userDn
     *            the user DN
     * @param system
     *            the system name
     * @param queryLogic
     *            the query logic
     * @return the response
     */
    public QueryLimiterResponse checkForLimits(String userDn, String system, String queryLogic) throws Exception {
        if (log.isTraceEnabled()) {
            log.trace("Checking limits - userDn: " + userDn + ", system: " + system + ", queryLogic: " + queryLogic);
        }

        ActiveQuerySnapshot snapshot = getActiveQuerySnapshot(userDn, system, queryLogic);

        int totalQueriesForUser = getTotalQueriesThatCountAgainstLimit(snapshot);
        boolean userOverrodeQueryLogicLimit = false;

        // Check if the user has reached their max concurrent query limit across all systems.
        UserQueryLimit userLimit = userLimitProvider.getLimit(userDn);
        if (totalQueriesForUser >= userLimit.getQueryLimit()) {
            String message = "User '" + userDn + "' has reached max " + (userLimit.getSource() == LimitSource.DEFAULTS ? "default " : "")
                            + "user query limit of " + userLimit.getQueryLimit();
            return QueryLimiterResponse.exceedsLimit(message);
        }

        // Check if the user has reached their max concurrent query limit for any overridden query logic groups.
        if (userLimit.overridesAnyQueryLogicLimits()) {
            Optional<QueryLogicGroupQueryLimit> optionalGroupLimit = queryLogicGroupLimitProvider.getOverriddenLimit(userDn, queryLogic,
                            userLimit.getQueryLogicGroupLimits());
            if (optionalGroupLimit.isPresent()) {
                userOverrodeQueryLogicLimit = true;
                QueryLogicGroupQueryLimit groupLimit = optionalGroupLimit.get();
                if (snapshot.getTotalUserQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "User '" + userDn + "' has reached max user query limit of " + groupLimit.getQueryLimit() + " for query logic "
                                    + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }

        // Check if the system has reached its max concurrent query limit.
        SystemQueryLimit systemLimit = systemLimitProvider.getLimit(system);
        if (snapshot.getTotalSystemQueries() >= systemLimit.getQueryLimit()) {
            String message = "System '" + system + "' has reached max " + (systemLimit.getSource() == LimitSource.DEFAULTS ? "default " : "")
                            + "system query limit of " + systemLimit.getQueryLimit();
            return QueryLimiterResponse.exceedsLimit(message);
        }

        // Check if the system has reached its max concurrent query limit for any overridden query logic groups.
        if (systemLimit.overridesAnyQueryLogicLimits()) {
            Optional<QueryLogicGroupQueryLimit> optionalGroupLimit = queryLogicGroupLimitProvider.getOverriddenLimit(system, queryLogic,
                            systemLimit.getQueryLogicGroupLimits());
            if (optionalGroupLimit.isPresent()) {
                QueryLogicGroupQueryLimit groupLimit = optionalGroupLimit.get();
                if (snapshot.getTotalSystemQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "System '" + system + "' has reached max query limit of " + groupLimit.getQueryLimit() + " for query logic " + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }

        // If the user did not have any overridden query logic groups that matched the query logic, check if the user has reached the max concurrent query limit
        // for a query logic group's default limit.
        if (!userOverrodeQueryLogicLimit) {
            Optional<QueryLogicGroupQueryLimit> optionalGroupLimit = queryLogicGroupLimitProvider.getLimit(queryLogic);
            if (optionalGroupLimit.isPresent()) {
                QueryLogicGroupQueryLimit groupLimit = optionalGroupLimit.get();
                if (snapshot.getTotalUserQueriesForQueryLogic() >= groupLimit.getQueryLimit()) {
                    String message = "User '" + userDn + "' has reached max default query limit of " + groupLimit.getQueryLimit() + " for query logic "
                                    + queryLogic;
                    return QueryLimiterResponse.exceedsLimit(message);
                }
            }
        }

        // The query can be submitted.
        return QueryLimiterResponse.doesNotExceedLimit();
    }

    /**
     * Return the total queries the user has running that count towards their query limit.
     *
     * @param snapshot
     *            the snapshot to extract query metrics from
     * @return the total queries
     */
    private int getTotalQueriesThatCountAgainstLimit(ActiveQuerySnapshot snapshot) {
        // The total number of queries a user has running, and the total number of those queries that count towards the user's query limit may differ. It
        // depends on the systems the queries were submitted on, and whether queries on the systems count towards the user's query limit. Evaluate each system
        // that the user has queries on, and only sum up the queries on systems that count towards the user's query limit.
        int totalQueries = 0;
        Map<String,Integer> totalUserQueriesPerSystem = snapshot.getTotalUserQueriesPerSystem();
        for (Map.Entry<String,Integer> entry : totalUserQueriesPerSystem.entrySet()) {
            String system = entry.getKey();
            if (systemLimitProvider.countsAgainstUserLimit(system)) {
                totalQueries += entry.getValue();
            }
        }
        return totalQueries;
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
        return getActiveQueryTracker().getSnapshot(userDn, system, queryLogic);
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
        trackQuery(queryId, userDn, getHostname(), queryLogic);
    }

    /**
     * Track the following information for the given query on Zookeeper.
     *
     * @param queryId
     *            the queryId
     * @param userDn
     *            the userDN of the user who submitted the query
     * @param system
     *            the system the query was submitted from
     * @param queryLogic
     *            the queryLogic the query is based on
     * @throws Exception
     *             if an error occurs
     */
    public void trackQuery(String queryId, String userDn, String system, String queryLogic) throws Exception {
        getActiveQueryTracker().trackQuery(queryId, userDn, system, queryLogic);
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

    private String getHostname() {
        return HOSTNAME;
    }
}
