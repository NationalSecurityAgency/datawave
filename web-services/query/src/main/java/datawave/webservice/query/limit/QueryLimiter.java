package datawave.webservice.query.limit;

import java.util.HashSet;
import java.util.List;
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
        String system = hostnameProvider.getCanonicalHostname();
        boolean systemCountsTowardsUserLimits = systemLimitProvider.countsAgainstUserLimit(system);
        
        QueryHeartbeat heartbeat = getActiveQueryTracker().trackQuery(queryId, userDn, hostnameProvider.getCanonicalHostname(), queryLogic, systemCountsTowardsUserLimits);
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
        
        public void checkLimits() throws Exception {
            checkUserLimits();
            if(metLimit) {
                return;
            }
            
            checkSystemLimits();
        }
        
        private void checkUserLimits() throws Exception {
            if(userLimitProvider.hasCustomLimits(userDn)) {
                checkCustomUserLimits();
            } else {
                checkDefaultQueryLogicLimits();
            }
        }
        
        private void checkCustomUserLimits() throws Exception {
            UserLimits userLimits = userLimitProvider.getCustomLimits(userDn);
            Map<String, Integer> groupLimits;
            if (userLimits.overridesAnyGroupLimits()) {
                groupLimits = userLimits.getBestGroupLimits(queryLogic);
            } else {
                groupLimits = queryLogicGroupLimitProvider.getGroupLimits(queryLogic);
            }
            checkUserLimits(groupLimits, userLimits.getQueryLimit());
        }
        
        private void checkDefaultQueryLogicLimits() throws Exception {
            Map<String, Integer> groupLimits = queryLogicGroupLimitProvider.getGroupLimits(queryLogic);
            checkUserLimits(groupLimits, userLimitProvider.getDefaultUserQueryLimit());
        }
        
        private void checkUserLimits(Map<String, Integer> groupLimits, int queryLimit) throws Exception {
            Set<String> queryLogics = new HashSet<>();
            int totalUserQueries = 0;
            ActiveQueryTracker tracker = getActiveQueryTracker();
            
            if(!groupLimits.isEmpty()) {
                Set<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);
                loadDistinctQueryLogics();
                limitCheckers.forEach(limitChecker -> queryLogics.addAll(limitChecker.matcher.getMatches(distinctQueryLogics)));
                
                for(String queryLogic : queryLogics) {
                    int totalQueriesForQueryLogic = tracker.getTotalUserQueriesForQueryLogic(userDn, queryLogic);
                    for(QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                        limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                        if (limitChecker.limitMet()) {
                            this.metLimit = true;
                            this.message = "User '" + userDn + "' has reached limit of " + limitChecker.limit + " running queries for query logic group '" + limitChecker.group + "'";
                            return;
                        }
                    }
                    
                    totalUserQueries += totalQueriesForQueryLogic;
                    if(totalUserQueries >= queryLimit) {
                        this.metLimit = true;
                        this.message = "User '" + userDn + "' has reached limit of " + queryLimit + " running queries";
                        return;
                    }
                }
            }
            
            if(tracker.totalUserQueriesMeetsLimit(userDn, queryLimit, queryLogics, totalUserQueries)) {
                this.metLimit = true;
                this.message = "User '" + userDn + "' has reached limit of " + queryLimit + " running queries";
            }
        }
        
        private void checkSystemLimits() throws Exception {
            Optional<SystemLimits> optional = systemLimitProvider.getCustomLimits(system);
            Set<String> queryLogics = new HashSet<>();
            int totalSystemQueries = 0;
            int queryLimit = systemLimitProvider.getDefaultSystemQueryLimit();
            
            ActiveQueryTracker tracker = getActiveQueryTracker();
            if(optional.isPresent()) {
                SystemLimits systemLimits = optional.get();
                queryLimit = systemLimits.getQueryLimit();
                if(systemLimits.overridesAnyGroupLimits()) {
                    Map<String, Integer> groupLimits = systemLimits.getRelevantGroupLimits(queryLogic);
                    if(!groupLimits.isEmpty()) {
                        Set<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);
                        loadDistinctQueryLogics();
                        limitCheckers.forEach(limitChecker -> queryLogics.addAll(limitChecker.matcher.getMatches(distinctQueryLogics)));
                        
                        for(String queryLogic : queryLogics) {
                            int totalQueriesForQueryLogic = tracker.getTotalSystemQueriesForQueryLogic(system, queryLogic);
                            for(QueryLogicGroupLimitChecker limitChecker : limitCheckers) {
                                limitChecker.incrementTotal(queryLogic, totalQueriesForQueryLogic);
                                if (limitChecker.limitMet()) {
                                    this.metLimit = true;
                                    this.message = "System '" + system + "' has reached limit of " + limitChecker.limit +
                                                    " running queries for query logic group '" + limitChecker.group + "'";
                                    return;
                                }
                            }
                            
                            totalSystemQueries += totalQueriesForQueryLogic;
                            if(totalSystemQueries >= queryLimit) {
                                this.metLimit = true;
                                this.message = "System '" + system + "' has reached limit of " + queryLimit + " running queries";
                                return;
                            }
                        }
                    }
                }
            }
            
            if (tracker.totalSystemQueriesMeetsLimit(system, queryLimit, queryLogics, totalSystemQueries)) {
                this.metLimit = true;
                this.message = "System '" + system + "' has reached limit of " + queryLimit + " running queries";
            }
            
        }
        
        private Set<QueryLogicGroupLimitChecker> getQueryLogicLimitCheckers(Map<String, Integer> groupsToLimits) {
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
            if(distinctQueryLogics == null) {
                distinctQueryLogics = getActiveQueryTracker().getDistinctQueryLogics();
            }
        }
    }
    
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
            if(matches(queryLogic)) {
                this.total = this.total + total;
            }
        }
        
        public boolean limitMet() {
            return total >= limit;
        }
        
    }
}
