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
import datawave.zookeeper.ZkClientBuilder;

/**
 * The implementation of {@link QueryLimiter}.
 */
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Singleton
@Startup
public class QueryLimiterImpl implements QueryLimiter {

    private enum State {
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
     * A lock that will block access to operations for this {@link QueryLimiterImpl} while {@link #setup()} or {@link #shutdown()} is executing.
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
                log.debug("Initializing limiter");
            }

            // Activate the limiter.
            activate();
        } catch (Exception e) {
            throw new RuntimeException(e);
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
    private void activate() throws Exception {
        lock.writeLock().lock();
        try {
            log.debug("Activating query limiter with configuration {}", this.config);

            // Validate the configuration.
            validateConfiguration();

            // Create the limit providers.
            QueryLimitConfiguration limitConfiguration = this.config.getLimitConfiguration();
            this.queryLogicGroupLimitProvider = new QueryLogicGroupLimitProvider(limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getQueryLogicGroupConfigs());
            this.userLimitProvider = new UserLimitProvider(limitConfiguration.getDefaultUserQueryLimit(), limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getUserConfigs(), queryLogicGroupLimitProvider);
            this.systemLimitProvider = new SystemLimitProvider(limitConfiguration.getDefaultSystemQueryLimit(), limitConfiguration.getInternalCacheMaxSize(),
                            limitConfiguration.getSystemConfigs(), queryLogicGroupLimitProvider);

            // If the Zookeeper client is null, initialize it.
            if (this.zkClient == null) {
                ZkClientBuilder clientBuilder = config.getZkClientBuilder();
                if (clientBuilder == null) {
                    throw new IllegalStateException("Zookeeper client builder cannot be null");
                }
                log.debug("Creating Zookeeper client with namespace {} using builder {}", QueryLimiterUtils.ZOOKEEPER_NAMESPACE, clientBuilder);
                this.zkClient = clientBuilder.duplicate().withNamespace(QueryLimiterUtils.ZOOKEEPER_NAMESPACE).build();
                this.zkClient.start();
                boolean connected = this.zkClient.blockUntilConnected(this.config.getZkClientConnectTimeout(), this.config.getZkClientConnectTimeoutUnit());
                if (!connected) {
                    // If we did not connect within the timeout, close the client and throw an exception.
                    this.zkClient.close();
                    throw new IllegalStateException("Zookeeper client failed to connect within timeout of " + this.config.getZkClientConnectTimeout() + " "
                                    + this.config.getZkClientConnectTimeoutUnit());
                }
            }

            // If the heartbeat cache is null, initialize it.
            if (heartbeatCache == null) {
                this.heartbeatCache = new QueryHeartbeatCache(this.config.getHeartbeatCleanupInterval(), this.config.getHeartbeatCleanupTimeUnit());
            }

            // Mark the limiter as active.
            setState(State.ACTIVE);
        } catch (Exception e) {
            log.error("Error when activating query limiter", e);
            deactivate(getState().getActivationFailureState());
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Validate the configuration of this {@link QueryLimiterImpl} instance.
     */
    private void validateConfiguration() {
        lock.readLock().lock();
        try {
            Preconditions.checkState(this.config != null, "Configuration has not been set");
            QueryLimitConfiguration limitConfiguration = this.config.getLimitConfiguration();
            Preconditions.checkState(limitConfiguration != null, "Limit configuration has not been set");
            Preconditions.checkState(limitConfiguration.getDefaultUserQueryLimit() > 0, "Default user query limit must be greater than 0");
            Preconditions.checkState(limitConfiguration.getInternalCacheMaxSize() > 0, "Internal max cache size must be greater than 0");
            Preconditions.checkState(this.config.getZkClientBuilder() != null, "Zookeeper client builder has not been set");
            Preconditions.checkState(this.config.getHeartbeatCleanupInterval() > 0, "Heartbeat cleanup interval must be greater than 0");
            Preconditions.checkState(this.config.getZkClientConnectTimeout() > 0, "Zookeeper client connect timeout must be greater than 0");
            Preconditions.checkState(this.config.getZkClientConnectTimeoutUnit() != null, "Zookeeper client connect timeout unit must not be null");
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Deactivate this {@link QueryLimiterImpl}. This method specifically does not close the heartbeat cache or Zookeeper client, that should only be done when
     * {@link #shutdown()} is called.
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
            log.error("Error when deactivating query limiter", e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Close this {@link QueryLimiterImpl} and the underlying zookeeper client.
     */
    @PreDestroy
    public void shutdown() {
        lock.writeLock().lock();
        try {
            log.debug("Shutting down limiter");
            deactivate(State.CLOSED);

            // Shut down the heartbeat cache, and stop all heartbeats.
            if (this.heartbeatCache != null) {
                try {
                    log.debug("Shutting down heartbeat cache");
                    this.heartbeatCache.close(true);
                } catch (Exception e) {
                    log.error("Error when shutting down heartbeat cache", e);
                } finally {
                    this.heartbeatCache = null;
                }
            }

            // Close the zookeeper client.
            if (this.zkClient != null) {
                try {
                    log.debug("Shutting down Zookeeper client");
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
     *             if the query limiter has never been initialized or has been closed
     */
    @Override
    public QueryLimiterResponse checkLimits(String userDn, String system, String queryLogic) throws Exception {
        userDn = QueryLimiterUtils.normalizeUserDn(userDn);
        system = QueryLimiterUtils.normalizeSystem(system);
        queryLogic = QueryLimiterUtils.normalizeQueryLogic(queryLogic);

        lock.readLock().lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Checking limits - userDn: {}, system: {}, queryLogic: {}", userDn, system, queryLogic);
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
                    log.debug("Query limiter is in state {}, returning no limit met by default", state);
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
     *             if the query limiter has never been initialized or has been closed
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
                if (log.isDebugEnabled()) {
                    log.debug("Marking query active: queryId={}, userDn={}, system={}, queryLogic={}", queryId, userDn, system, queryLogic);
                }
                boolean systemCountsTowardsUserLimits = systemLimitProvider.countsAgainstUserLimit(system);
                QueryHeartbeat heartbeat = getActiveQueryTracker().trackQuery(queryId, userDn, system, queryLogic, systemCountsTowardsUserLimits);
                // Store the heartbeat into the cache. This acts as a means to keep the connection to Zookeeper alive for the ephemeral nodes stored in the
                // heartbeat.
                heartbeatCache.put(heartbeat);
            } else {
                State state = getState();
                if (state.interactionAllowed) {
                    log.debug("Query limiter is in state {}, query {} will not be marked active.", state, queryId);
                } else {
                    throw new IllegalStateException("Marking queries not allowed while limiter is in state " + state);
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
     *             if the query limiter has never been initialized or has been closed
     */
    @Override
    public Set<String> getActiveQueries() {
        lock.readLock().lock();
        try {
            State state = getState();
            // Even if the limiter is disabled, there may be active queries lingering from when the query limiter was previously active. Allow callers to
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
     *             if the query limiter has never been initialized or has been closed
     */
    @Override
    public void markInactive(Collection<String> queryIds) {
        Preconditions.checkNotNull(queryIds, "queryIds cannot be null");
        lock.readLock().lock();
        try {
            if (log.isDebugEnabled()) {
                log.debug("Stopping counting queries towards limits: {}", queryIds);
            }
            State state = getState();
            // Even if the limiter is disabled, there may be active queries lingering from when the query limiter was previously active. Allow callers to mark
            // these queries as inactive.
            if (isEnabled() || state.interactionAllowed) {
                heartbeatCache.stopAndRemove(queryIds);
            } else {
                throw new IllegalStateException("Stopping active queries not allowed while limiter is in state " + state);
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
            if (log.isDebugEnabled()) {
                log.debug("Stop counting query {} towards limits", queryId);
            }
            State state = getState();
            // Even if the limiter is disabled, the given query may be lingering from when the query limiter was previously active. Allow callers to mark
            // the query as inactive.
            if (isEnabled() || state.interactionAllowed) {
                heartbeatCache.stopAndRemove(queryId);
            } else {
                throw new IllegalStateException("Stopping active queries not allowed while limiter is in state " + state);
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
    private boolean isEnabled() {
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
    private State getState() {
        lock.readLock().lock();
        try {
            return state.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return the {@link ActiveQueryTracker} instance, initializing it if needed.
     *
     * @return the active query tracker
     */
    private ActiveQueryTracker getActiveQueryTracker() {
        if (this.activeQueryTracker == null) {
            this.activeQueryTracker = new ActiveQueryTracker(zkClient);
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
        private void loadDistinctQueryLogics() {
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
