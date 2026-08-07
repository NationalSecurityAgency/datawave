package datawave.webservice.query.limit;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import datawave.configuration.spring.SpringBean;
import datawave.zookeeper.ZkClientBuilder;
import datawave.zookeeper.ZkPojoPublisher;
import org.apache.curator.framework.CuratorFramework;

import com.google.common.base.Preconditions;

import datawave.zookeeper.ZkPojoPublisherImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

/**
 * This class is responsible for determining if any concurrent query limits are going to be exceeded for a user, system, or query logic when a new query is
 * submitted. It is expected that only a singleton instance of {@link QueryLimiter} will be created via CDI.
 */
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Singleton
@Startup
public class QueryLimiter {

    private static final Logger log = LoggerFactory.getLogger(QueryLimiter.class);
    
    /**
     * The Zookeeper namespace that active query nodes will be stored under.
     */
    public static final String NAMESPACE = "ActiveQueries";
    
    /**
     * The default value to use as a system name when no system is provided with a query.
     */
    public static final String EMPTY_SYSTEM_FROM = "EMPTY_SYSTEM_FROM";

    /**
     * A lock that guards read/write access to properties of this {@link QueryLimiter}.
     */
    private final ReadWriteLock limiterLock = new ReentrantReadWriteLock();
    
    /**
     * Whether this {@link QueryLimiter} is considered activated.
     */
    private final AtomicBoolean activated = new AtomicBoolean(false);

    /**
     * The Zookeeper client builder.
     */
    @Inject
    @SpringBean(name = "defaultZkClientBuilder")
    @SuppressWarnings("CdiInjectionPointsInspection")
    private ZkClientBuilder zkClientBuilder;
    
    /**
     * The configuration to initialize the limit providers with.
     */
    @Inject
    @SpringBean
    @SuppressWarnings("CdiInjectionPointsInspection")
    private QueryLimitConfiguration configuration;
    
    /**
     * A cache to store heartbeats of active queries within.
     */
    @Inject
    @SpringBean
    @SuppressWarnings("CdiInjectionPointsInspection")
    private QueryHeartbeatCache heartbeatCache;
    
    /**
     * The publisher that the query limiter will listen to for updates to the configuration.
     */
    @Inject
    @Qualifier("queryLimitConfigPublisher")
    private ZkPojoPublisher<QueryLimitConfiguration> configPublisher;
    
    /**
     * The listener for configuration updates. If this is not null, we are listening for updates.
     */
    private Consumer<QueryLimitConfiguration> configUpdateListener;
    
    /**
     * Provides configured limits for query logic groups.
     */
    private QueryLogicGroupLimitProvider queryLogicGroupLimitProvider;
    
    /**
     * Provides configured limits for users.
     */
    private UserLimitProvider userLimitProvider;
    
    /**
     * Provides configured limits for systems.
     */
    private SystemLimitProvider systemLimitProvider;
    
    /**
     * The tracker responsible for creating nodes in Zookeeper that track active queries.
     */
    private ActiveQueryTracker activeQueryTracker;

    /**
     * The Zookeeper client.
     */
    private CuratorFramework client;

    /**
     * Set the zookeeper connection string
     *
     * @param zkClientBuilder
     *            the zookeeper connection string
     */
    public void setZkClientBuilder(ZkClientBuilder zkClientBuilder) {
        this.zkClientBuilder = zkClientBuilder;
    }
    
    /**
     * Set the config publisher that will notify this {@link QueryLimiter} of configuration updates.
     *
     * @param configPublisher
     *            the configuration publisher
     */
    public void setConfigPublisher(ZkPojoPublisher<QueryLimitConfiguration> configPublisher) {
        this.configPublisher = configPublisher;
    }

    /**
     * Set the configuration for the {@link QueryLimiter}.
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
        limiterLock.writeLock().lock();
        try {
            this.configuration = queryLimitConfiguration;
        } finally {
            limiterLock.writeLock().unlock();
        }
    }

    /**
     * Return the configuration currently configured for this {@link QueryLimiter}.
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
            log.debug("Setting up query limiter.");
        }

        limiterLock.writeLock().lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Setting up with zkClientBuilder={}, configuration={}", zkClientBuilder,  configuration);
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
        log.debug("Activating query limiter.");
        try {
            // Validate the configuration.
            ValidationUtils.validateQueryLimitConfig(configuration);

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
                // Ensure that we are always using the correct namespace when building the client.
                this.client = zkClientBuilder.duplicate().withNamespace(NAMESPACE).buildAndStart(3, TimeUnit.MINUTES);
            }

            // The active query tracker instance can be reused between configuration updates.
            if (this.activeQueryTracker == null) {
                this.activeQueryTracker = new ActiveQueryTracker(client);
            }

            // Listen for configuration updates. We only need to do this once. Any exception thrown by the subscribing consumer will be captured by the POJO
            // publisher and written to Zookeeper as part of the attempt status.
            if (this.configUpdateListener == null && this.configPublisher != null) {
                this.configUpdateListener = createConfigUpdateConsumer();
                this.configPublisher.addListener(this.configUpdateListener);
            }

            this.activated.set(true);
        } catch (Exception e) {
            log.error("Activation failed. Deactivating query limiter.", e);
            clear();
            throw new QueryLimitException("Activation failed.", e);
        } finally {
            limiterLock.writeLock().unlock();
        }
    }
    
    /**
     * Return a new {@link Consumer<Object>} that can be registered with a {@link ZkPojoPublisherImpl} to update this {@link QueryLimiter} when a new
     * {@link QueryLimitConfiguration} is published. The update steps will be as follows:
     * <ol>
     * <li>Verify the object provided by the publisher is a {@link QueryLimitConfiguration}.</li>
     * <li>Verify the new configuration is valid.</li>
     * <li>Update the configuration and call {@link #setup()}.</li>
     * <li>If the previous step fails, restore the old configuration and call {@link #setup()}.</li>
     * </ol>
     * @return the consumer
     */
    private Consumer<QueryLimitConfiguration> createConfigUpdateConsumer() {
        return newConfig -> {
            if(log.isDebugEnabled()) {
                log.debug("Received config update from publisher: {}", newConfig);
            }
            
            // Validate the new configuration.
            try {
                ValidationUtils.validateQueryLimitConfig(newConfig);
            } catch (Exception e) {
                log.error("New configuration failed validation. Configuration will not be updated.", e);
                return;
            }
            
            // Keep a backup copy of the old configuration.
            QueryLimitConfiguration oldConfig = this.configuration;
            
            try {
                // Attempt to update the query limiter using the new configuration.
                setConfiguration(newConfig);
                setup();
                if(log.isDebugEnabled()) {
                    log.debug("New configuration applied. Query limits enforced: {}", isEnforcingLimits());
                }
            } catch (Exception newConfigException) {
                log.error("Failed to apply new configuration. Reverting back to old configuration: {}", oldConfig);
                try {
                    // If the update failed for any reason, attempt to update the limiter with the old configuration.
                    setConfiguration(oldConfig);
                    setup();
                    if(log.isDebugEnabled()) {
                        log.debug("Old configuration restored. Query limits enforced: {}", isEnforcingLimits());
                    }
                    throw new ConfigurationUpdateFailedException("Failed to apply new configuration. Old configuration restored.", newConfigException);
                } catch (ConfigurationUpdateFailedException e) {
                    // If a ConfigurationUpdateFailedException was thrown, we know the old configuration was successfully restored. Just throw this exception.
                    throw e;
                } catch (Exception e) {
                    log.error("Failed to restore old configuration. Disabling query limiter.", e);
                    // An exception was thrown when trying to restore the old configuration. Ensure activated is false to suspend query limit enforcement.
                    this.activated.set(false);
                    // Throw an exception that includes the exception thrown when trying to update the limiter with the new configuration.
                    ConfigurationUpdateFailedException exception = new ConfigurationUpdateFailedException(
                                    "Failed to restore old configuration after failing to apply new configuration. Query limiter is disabled.", e);
                    exception.addSuppressed(newConfigException);
                }
            }
        };
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
        log.debug("Clearing query limiter.");
        try {
            this.activated.set(false);

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
     * Releases internal resources and cleans up connections and scheduled tasks.
     */
    public void shutdown() {
        limiterLock.writeLock().lock();
        log.debug("Shutting down query limiter.");
        try {
            clear();
            
            if (this.client != null) {
                try {
                    this.client.close();
                } catch (Exception e) {
                    log.warn("Error closing Zookeeper client", e);
                } finally {
                    this.client = null;
                }
            }
            
            this.activeQueryTracker = null;
        } finally {
            limiterLock.writeLock().unlock();
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
        // Use a read lock that allows for concurrent limit checking, but blocks if the query limiter configuration is being updated.
        limiterLock.readLock().lock();
        try {
            if (isEnforcingLimits()) {
                // Cast the user DN to lowercase to ensure a consistent format.
                userDn = userDn.trim().toLowerCase();

                // Do not cast the system or query logic to lowercase, they will be getting matched against regex patterns.
                queryLogic = queryLogic.trim();

                // Ensure the system is non-null if empty
                if (system == null || system.isBlank()) {
                    system = EMPTY_SYSTEM_FROM;
                }

                if (log.isDebugEnabled()) {
                    log.debug("Checking limits - userDn: {}, system: {}, queryLogic: {}", userDn, system, queryLogic);
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
                    log.debug("Query limits are not being enforced.");
                }
                return QueryLimiterResponse.hasNotMetLimit();
            }
        } finally {
            limiterLock.readLock().unlock();
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
        // Use a read lock that allows for concurrent query tracking, but blocks if the query limiter configuration is being updated.
        limiterLock.readLock().lock();
        try {
            // If the system limit provider is not null, the query limiter is configured and able to track queries.
            if (systemLimitProvider != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Start counting query {} towards limits", queryId);
                }

                userDn = userDn.trim().toLowerCase();
                queryLogic = queryLogic.trim();
                // Ensure the system is non-null if empty
                if (system == null || system.isBlank()) {
                    system = EMPTY_SYSTEM_FROM;
                }

                boolean systemCountsTowardsUserLimits = systemLimitProvider.countsAgainstUserLimit(system);
                QueryHeartbeat heartbeat = activeQueryTracker.trackQuery(queryId, userDn, system, queryLogic, systemCountsTowardsUserLimits);
                // Store the heartbeat into the cache. This acts as a means to keep the connection to Zookeeper alive for the ephemeral nodes stored in the
                // heartbeat.
                heartbeatCache.put(heartbeat);
            } else {
                log.warn("Query limiter is not configured. Cannot track query {}", queryId);
            }
        } finally {
            limiterLock.readLock().unlock();
        }
    }

    /**
     * Return whether this {@link QueryLimiter} is currently enforcing limits.
     *
     * @return true if this {@link QueryLimiter} is enforcing limits, or false otherwise
     */
    public boolean isEnforcingLimits() {
        return activated.get();
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
            log.debug("Stopping counting queries towards limits: {}", queryIds);
        }
        // Use a read lock that allows for concurrent tracking cancellations, but blocks if the query limiter configuration is being updated.
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
            log.debug("Stop counting query {} towards limits", queryId);
        }
        // Use a read lock that allows for concurrent tracking cancellations, but blocks if the query limiter configuration is being updated.
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
            // If groupsLimit is not empty, then we found one or more best-matching groups for the query logic.
            if (!groupLimits.isEmpty()) {
                // Create a set of limit checkers for each query logic group.
                Set<QueryLogicGroupLimitChecker> limitCheckers = getQueryLogicLimitCheckers(groupLimits);

                // Load the distinct query logics, and fetch all query logics that fall within the target groups.
                loadDistinctQueryLogics();
                limitCheckers.forEach(limitChecker -> queryLogics.addAll(limitChecker.matcher.getMatches(distinctQueryLogics)));

                // Fetch the total running queries for each query logic for the user.
                for (String queryLogic : queryLogics) {
                    int totalQueriesForQueryLogic = activeQueryTracker.getTotalUserQueriesForQueryLogic(userDn, queryLogic);
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
            if (activeQueryTracker.totalUserQueriesMeetsLimit(userDn, queryLimit, queryLogics, totalUserQueries)) {
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
                            int totalQueriesForQueryLogic = activeQueryTracker.getTotalSystemQueriesForQueryLogic(system, queryLogic);
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
                if (activeQueryTracker.totalSystemQueriesMeetsLimit(system, queryLimit, queryLogics, totalSystemQueries)) {
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
        private void loadDistinctQueryLogics() {
            if (distinctQueryLogics == null) {
                distinctQueryLogics = activeQueryTracker.getDistinctQueryLogics();
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
