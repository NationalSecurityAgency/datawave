package datawave.webservice.query.limit;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.configuration.spring.SpringBean;

/**
 * The implementation of {@link QueryLimiter}.
 */
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Singleton
@Startup
public class QueryLimiterImpl implements QueryLimiter {
    
    private static final Logger log = LoggerFactory.getLogger(QueryLimiterImpl.class);

    /**
     * The Zookeeper client.
     */
    private CuratorFramework zkClient;

    /**
     * The configuration for this {@link QueryLimiterImpl}.
     */
    @Inject
    @SpringBean(name = "queryLimiterConfig")
    @SuppressWarnings("CdiInjectionPointsInspection")
    private QueryLimiterImplConfiguration config;

    /**
     * The cache of query heartbeats.
     */
    private QueryHeartbeatCache heartbeatCache;

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
     * The tracker responsible for interfacing with Zookeeper.
     */
    private ActiveQueryTracker activeQueryTracker;

    /**
     * Set the configuration for this {@link QueryLimiterImpl}. The configuration within will not be applied until {@link #setup()} is called.
     *
     * @param config
     *            the configuration
     */
    public void setConfiguration(QueryLimiterImplConfiguration config) {
        lock.writeLock().lock();
        try {
            this.config = config == null ? null : config.deepCopy();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * A lock that will block access to operations for this {@link QueryLimiterImpl} while {@link #setup()} or {@link #close()} is executing.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * The current state of the {@link QueryLimiterImpl}.
     */
    private final AtomicReference<State> state = new AtomicReference<>(State.UNINITIALIZED);

    /**
     * Set up this {@link QueryLimiterImpl} instance.
     */
    @PostConstruct
    public void setup() {
        lock.writeLock().lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Setting up limiter");
            }

            // Attempt to activate the limiter.
            activate();
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("State after setup: {}, limiter enabled: {}", getState(), isEnabled());
            }
            lock.writeLock().unlock();
        }
    }

    /**
     * Attempt to activate this {@link QueryLimiterImpl} instance.
     */
    private void activate() {
        lock.writeLock().lock();
        try {
            if(log.isDebugEnabled()) {
                log.debug("Activating limiter with configuration {}", this.config);
            }

            Preconditions.checkNotNull(this.config, "Limiter configuration cannot be null");
            
            initLimitProviders();
            initZkClient();
            initHeartbeatCache();
            initActiveQueryTracker();
            
            // Mark the limiter as active.
            setState(State.ACTIVE);
        } catch (Exception e) {
            log.error("Failed to activate limiter", e);
            deactivate(getState().getActivationFailureState());
            throw new RuntimeException("Failed to activate limiter", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Initialize the limit providers.
     */
    private void initLimitProviders() {
        lock.writeLock().lock();
        try {
            if(log.isDebugEnabled()) {
                log.debug("Creating limit providers from query limit configuration {}", this.config.getLimitConfiguration());
            }
            
            QueryLimitConfiguration limitConfiguration = this.config.getLimitConfiguration();
            Preconditions.checkNotNull(limitConfiguration, "Query limit configuration cannot be null");
            Preconditions.checkArgument(limitConfiguration.getDefaultUserQueryLimit() > 0, "Default user query limit must be greater than 0");
            Preconditions.checkArgument(limitConfiguration.getInternalCacheMaxSize() > 0, "Internal max cache size must be greater than 0");
            
            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(limitConfiguration.getDefaultUserQueryLimit(), limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getUserConfigs(), queryLogicGroupLimitProvider);
            this.systemLimitProvider = new SystemLimitProvider(limitConfiguration.getDefaultSystemQueryLimit(), limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getSystemConfigs(), queryLogicGroupLimitProvider);
        } catch (Exception e) {
            log.error("Failed to initialize limit providers", e);
            throw new RuntimeException("Failed to initialize limit providers", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Initialize the Zookeeper client. This should only be done once in a {@link QueryLimiterImpl}'s lifetime.
     */
    private void initZkClient() {
        lock.writeLock().lock();
        try {
            // Initialize the Zookeeper client only if it hasn't been initialized yet.
            if (this.zkClient == null) {
                if(log.isDebugEnabled()) {
                    log.debug("Creating Zookeeper client with namespace {} and client connect time out of {} {} using client builder {}",
                                    QueryLimiterUtils.ZOOKEEPER_NAMESPACE, this.config.getZkClientConnectTimeout(),
                                    this.config.getZkClientConnectTimeoutUnit(), this.config.getZkClientBuilder());
                }
                
                Preconditions.checkNotNull(this.config.getZkClientBuilder(), "Zookeeper client builder cannot be null");
                Preconditions.checkArgument(this.config.getZkClientConnectTimeout() > 0, "Zookeeper client connect timeout must be greater than 0");
                Preconditions.checkNotNull(this.config.getZkClientConnectTimeoutUnit(), "Zookeeper client connect timeout unit cannot be null");
                
                this.zkClient = this.config.getZkClientBuilder().duplicate().withNamespace(QueryLimiterUtils.ZOOKEEPER_NAMESPACE).build();
                this.zkClient.start();
                boolean connected = this.zkClient.blockUntilConnected(this.config.getZkClientConnectTimeout(), this.config.getZkClientConnectTimeoutUnit());
                if (!connected) {
                    // If we did not connect within the timeout, close the client and throw an exception.
                    throw new IllegalStateException("Zookeeper client failed to connect within timeout of " + this.config.getZkClientConnectTimeout() + " "
                                    + this.config.getZkClientConnectTimeoutUnit());
                }
            } else {
                log.debug("Zookeeper client already exists, skipping initialization");
            }
        } catch (Exception e) {
            log.error("Failed to initialize Zookeeper client", e);
            // If an error occurred, ensure the Zookeeper client is closed to avoid leaking the connection.
            closeZkClient();
            throw new RuntimeException("Failed to initialize Zookeeper client", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Initialize the heartbeat cache. This should only be done once in a {@link QueryLimiterImpl}'s lifetime.
     */
    private void initHeartbeatCache() {
        lock.writeLock().lock();
        try {
            // Initialize the heart beat cache only if it hasn't been initialized yet.
            if (heartbeatCache == null) {
                if(log.isDebugEnabled()) {
                    log.debug("Creating heartbeat cache with periodic cleanup of stopped heartbeats every {} {}", config.getHeartbeatCleanupInterval(),
                                    config.getHeartbeatCleanupIntervalUnit());
                }
                this.heartbeatCache = new QueryHeartbeatCache(this.config.getHeartbeatCleanupInterval(), this.config.getHeartbeatCleanupIntervalUnit());
            } else {
                log.debug("Heartbeat cache already exists, skipping initialization");
            }
        } catch (Exception e) {
            log.error("Failed to initialize heartbeat cache", e);
            throw new RuntimeException("Failed to initialize heartbeat cache", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Initialize the active query tracker. This should only be done once in a {@link QueryLimiterImpl}'s lifetime.
     */
    private void initActiveQueryTracker() {
        lock.writeLock().lock();
        try {
            if(activeQueryTracker == null) {
                log.debug("Creating active query tracker");
                this.activeQueryTracker = new ActiveQueryTracker(this.zkClient);
            } else {
                log.debug("Active query tracker already exists, skipping initialization");
            }
        } catch (Exception e) {
            log.error("Failed to initialize active query tracker", e);
            throw new RuntimeException("Failed to initialize active query tracker", e);
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Deactivate this {@link QueryLimiterImpl}. This method specifically does not close the heartbeat cache or Zookeeper client, that should only be done when
     * {@link #close()} is called.
     */
    private void deactivate(State newState) {
        lock.writeLock().lock();
        try {
            log.debug("Deactivating limiter.");
            setState(newState);

            this.queryLogicGroupLimitProvider = null;
            this.systemLimitProvider = null;
            this.userLimitProvider = null;
        } catch (Exception e) {
            log.error("Error when deactivating limiter", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close this {@link QueryLimiterImpl} and releases its resources. Calling this method will result in the deletion of all ephemeral nodes used to track
     * active queries in Zookeeper.
     */
    @PreDestroy
    public void close() {
        lock.writeLock().lock();
        try {
            log.debug("Shutting down limiter");
            deactivate(State.CLOSED);

            // Shut down the heartbeat cache, and stop all heartbeats. This needs to be done before closing the Zookeeper client.
            log.debug("Closing heartbeat cache");
            if (this.heartbeatCache != null) {
                try {
                    // Ensure heartbeats in the cache are stopped after they're removed.
                    this.heartbeatCache.close(true);
                } catch (Exception e) {
                    log.error("Error when shutting down heartbeat cache", e);
                } finally {
                    this.heartbeatCache = null;
                }
            }
            
            // Nullify the active query tracker.
            this.activeQueryTracker = null;
            
            // Close the Zookeeper client.
            closeZkClient();
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    /**
     * Close the Zookeeper client.
     */
    private void closeZkClient() {
        lock.writeLock().lock();
        try {
            log.debug("Closing Zookeeper client");
            if(this.zkClient != null) {
                try {
                    this.zkClient.close();
                } catch (Exception e) {
                    log.error("Error closing Zookeeper client", e);
                } finally {
                    this.zkClient = null;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if the user is allowed to create another query based on the given query logic on the current system. If this {@link QueryLimiterImpl} is disabled,
     * a response indicating no limit met will be returned.
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
     * @throws IllegalStateException
     *             if the limiter has never been initialized or has been closed
     */
    @Override
    public QueryLimiterResponse checkLimits(String userDn, String system, String queryLogic) throws Exception {
        userDn = QueryLimiterUtils.normalizeUserDn(userDn);
        system = QueryLimiterUtils.normalizeSystem(system);
        queryLogic = QueryLimiterUtils.normalizeQueryLogic(queryLogic);

        lock.readLock().lock();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Checking limits - userDn: {}, system: {}, queryLogic: {}", userDn, system, queryLogic);
            }

            if (isEnabled()) {
                // Check if the snapshot reveals that any limits have been met.
                LimitChecker checker = new LimitChecker(userDn, system, queryLogic);
                checker.checkLimits();
                if (checker.metLimit) {
                    return QueryLimiterResponse.metLimit(checker.message);
                } else {
                    return QueryLimiterResponse.hasNotMetLimit();
                }
            } else {
                State state = getState();
                if (state.interactionAllowed) {
                    if(log.isTraceEnabled()) {
                        log.trace("Query limiter is in state {}, returning no limit met by default", state);
                    }
                    return QueryLimiterResponse.hasNotMetLimit();
                } else {
                    throw new IllegalStateException("Checking limits not allowed while limiter is in state " + state);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Track the following information for the given query on Zookeeper for the current system, and count it towards any configured query limits. If this
     * {@link QueryLimiterImpl} is disabled, the query will not be tracked as an active query.
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
     * @throws IllegalStateException
     *             if the limiter has never been initialized or has been closed
     */
    @Override
    public void markActive(String queryId, String userDn, String system, String queryLogic) throws Exception {
        Preconditions.checkArgument(queryId != null && !queryId.isBlank(), "queryId cannot be null or blank");
        userDn = QueryLimiterUtils.normalizeUserDn(userDn);
        system = QueryLimiterUtils.normalizeSystem(system);
        queryLogic = QueryLimiterUtils.normalizeQueryLogic(queryLogic);

        lock.readLock().lock();
        try {
            if (isEnabled()) {
                if (log.isTraceEnabled()) {
                    log.trace("Marking query active: queryId={}, userDn={}, system={}, queryLogic={}", queryId, userDn, system, queryLogic);
                }
                boolean systemCountsTowardsUserLimits = systemLimitProvider.countsAgainstUserLimit(system);
                QueryHeartbeat heartbeat = activeQueryTracker.trackQuery(queryId, userDn, system, queryLogic, systemCountsTowardsUserLimits);
                // Store the heartbeat into the cache. This acts as a means to keep the connection to Zookeeper alive for the ephemeral nodes stored in the
                // heartbeat.
                heartbeatCache.put(heartbeat);
            } else {
                State state = getState();
                if (state.interactionAllowed) {
                    if(log.isTraceEnabled()) {
                        log.trace("Query limiter is in state {}, query {} will not be marked active.", state, queryId);
                    }
                } else {
                    throw new IllegalStateException("Marking queries active not allowed while limiter is in state " + state);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

    }

    /**
     * Fetch the set of query IDs for queries considered to be actively running by the this {@link QueryLimiterImpl}.
     *
     * @return the set of IDs for active queries
     * @throws IllegalStateException
     *             if the limiter has never been initialized or has been closed
     */
    @Override
    public Set<String> getActiveQueries() {
        lock.readLock().lock();
        try {
            State state = getState();
            // Even if the limiter is disabled, there may be active queries lingering from when the limiter was previously active. Allow callers to
            // retrieve information about active queries.
            if (isEnabled() || state.interactionAllowed) {
                return heartbeatCache.getQueryIds();
            } else {
                throw new IllegalStateException("Fetching active queries not allowed while limiter is in state " + state);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear the information for each of the given queries from Zookeeper, and stop counting them towards any configured query limits.
     *
     * @param queryIds
     *            the query IDs
     * @throws IllegalStateException
     *             if the limiter has never been initialized or has been closed
     */
    @Override
    public void markInactive(Collection<String> queryIds) {
        Preconditions.checkNotNull(queryIds, "queryIds cannot be null");
        lock.readLock().lock();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Stopping counting queries towards limits: {}", queryIds);
            }
            State state = getState();
            // Even if the limiter is disabled, there may be active queries lingering from when the limiter was previously active. Allow callers to mark
            // these queries as inactive.
            if (isEnabled() || state.interactionAllowed) {
                heartbeatCache.stopAndRemove(queryIds);
            } else {
                throw new IllegalStateException("Marking queries inactive not allowed while limiter is in state " + state);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear the information for the given query from Zookeeper, and stop counting it towards any configured query limits.
     *
     * @param queryId
     *            the query ID
     */
    @Override
    public void markInactive(String queryId) {
        Preconditions.checkArgument(queryId != null && !queryId.isBlank(), "queryId cannot be null or blank");
        lock.readLock().lock();
        try {
            if (log.isTraceEnabled()) {
                log.trace("Stop counting query {} towards limits", queryId);
            }
            State state = getState();
            // Even if the limiter is disabled, the given query may be lingering from when the limiter was previously active. Allow callers to mark
            // the query as inactive.
            if (isEnabled() || state.interactionAllowed) {
                heartbeatCache.stopAndRemove(queryId);
            } else {
                throw new IllegalStateException("Marking queries inactive not allowed while limiter is in state " + state);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return whether this {@link QueryLimiterImpl} is considered enabled
     *
     * @return true if the limiter is enabled, or false otherwise
     */
    public boolean isEnabled() {
        lock.readLock().lock();
        try {
            return getState() == State.ACTIVE;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Set the state for this {@link QueryLimiterImpl}.
     *
     * @param newState
     *            the state to set
     */
    private void setState(State newState) {
        lock.writeLock().lock();
        try {
            State oldState = this.state.getAndSet(newState);
            if (log.isDebugEnabled()) {
                log.debug("Transitioned from state {} to state {}", oldState, newState);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the current state for this {@link QueryLimiterImpl}.
     *
     * @return the state
     */
    public State getState() {
        lock.readLock().lock();
        try {
            return state.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Represents the state of a {@link QueryLimiterImpl}.
     */
    public enum State {
        /**
         * Represents an uninitialized state.
         */
        UNINITIALIZED(false) {
            @Override
            State getActivationFailureState() {
                return UNINITIALIZED;
            }
        },
        
        /**
         * Represents an idle state where limits are not enforced, and active queries will not be tracked.
         */
        IDLE(true) {
            @Override
            State getActivationFailureState() {
                return IDLE;
            }
        },
        
        /**
         * Represents an active state where query limits are enforced, and active queries are tracked.
         */
        ACTIVE(true) {
            @Override
            State getActivationFailureState() {
                return IDLE;
            }
        },
        
        /**
         * Represents a closed state where the limiter has been shut down.
         */
        CLOSED(false) {
            @Override
            State getActivationFailureState() {
                return CLOSED;
            }
        };
        
        /**
         * Whether interaction with the limiter is allowed when it is in the current state.
         */
        private final boolean interactionAllowed;
        
        State(boolean interactionAllowed) {
            this.interactionAllowed = interactionAllowed;
        }
        
        /**
         * Return the {@link State} the limiter should transition to from this {@link State} if an error occurs during activation.
         *
         * @return the transition state
         */
        abstract State getActivationFailureState();
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
                            if (queryLimit != QueryLimiterUtils.NO_LIMIT) {
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
            if (queryLimit != QueryLimiterUtils.NO_LIMIT) {
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
