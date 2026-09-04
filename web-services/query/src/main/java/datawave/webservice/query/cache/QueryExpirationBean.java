package datawave.webservice.query.cache;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.DependsOn;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

import org.apache.log4j.Logger;

import datawave.configuration.spring.SpringBean;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
@Singleton
@DependsOn({"QueryMetricsBean", "AccumuloConnectionFactoryBean"})
@Lock(LockType.WRITE)
// by default all methods are blocking
public class QueryExpirationBean {

    private static final Logger log = Logger.getLogger(QueryExpirationBean.class);

    @Inject
    private QueryCache queryCache;

    @Inject
    @SpringBean(refreshable = true)
    private QueryExpirationProperties config;

    @Inject
    private AccumuloConnectionFactory connectionFactory;

    @Inject
    private CreatedQueryLogicCacheBean queryLogicCacheBean;

    @Inject
    private QueryMetricsBean metricsBean;

    @Inject
    @SpringBean(name = "queryLimiter")
    private QueryLimiter queryLimiter;

    private boolean clearAll = false;

    @PostConstruct
    public void init() {
        if (log.isDebugEnabled()) {
            log.debug("@PostConstruct - init()");
        }

        if (config == null) {
            throw new IllegalArgumentException("QueryExpirationProperties is null");
        }
    }

    @PreDestroy
    public void close() {
        if (log.isDebugEnabled()) {
            log.debug("@PreDestroy - Closing all active queries and query logics before shutdown.");
            log.debug("Overriding idle and call time thresholds to zero so that all queries and logics resources are cleared before shutdown.");
        }

        clearAll = true;
        clearQueries(System.currentTimeMillis());
    }

    /**
     * The cache eviction notifications are not working. Using an interceptor is not working either. This method will be invoked every 30 seconds by the timer
     * service and will evict entries that are idle or expired.
     */
    @Schedule(hour = "*", minute = "*", second = "*/30", persistent = false)
    public void removeIdleOrExpired() {
        if (log.isDebugEnabled()) {
            log.debug("@Schedule - removeIdleOrExpired");
        }
        long now = System.currentTimeMillis();
        clearQueries(now);
        queryLogicCacheBean.clearQueryLogics(now, config.getCallTimeoutMillis());
        synchronizeActiveQueries();
    }

    private void clearQueries(long now) {
        int count = 0;

        // Examine each query in the cache.
        for (RunningQuery query : queryCache) {
            // Check if the query is considered idle for too long or if the next call has been running for too long.
            boolean idleTooLong = !clearAll && isIdleTooLong(query, now);
            boolean nextTooLong = !clearAll && isNextTooLong(query, now);
            Query settings = query.getSettings();
            // If we are shutting down this bean, or the query is considered idle for too long or its next call has been running for too long, time out the
            // query and remove it from the cache.
            if (clearAll || idleTooLong || nextTooLong) {
                if (settings.getUncaughtExceptionHandler() == null) {
                    settings.setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
                }
                try {
                    BaseQueryMetric queryMetric = query.getMetric();
                    // If we are shutting down this bean, mark the query as shut down.
                    if (clearAll) {
                        queryMetric.setLifecycle(QueryMetric.Lifecycle.SHUTDOWN);
                        settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.SERVER_SHUTDOWN));
                    } else {
                        // If the query has been idle for too long or is taking too long to complete its next call, mark the query as timed out.
                        if (!queryMetric.isLifecycleFinal() && !query.isFinished()) {
                            if (idleTooLong) {
                                queryMetric.setLifecycle(BaseQueryMetric.Lifecycle.TIMEOUT);
                            } else if (nextTooLong) {
                                queryMetric.setLifecycle(BaseQueryMetric.Lifecycle.TIMEOUT);
                            }
                        }
                        settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.QUERY_TIMEOUT));
                    }
                } finally {
                    // If need be, update the metrics for the query.
                    if (query.getLogic().getCollectQueryMetrics()) {
                        try {
                            metricsBean.updateMetric(query.getMetric());
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }

                // Cancel the query if it is considered active.
                if (query.hasActiveCall()) {
                    query.cancel();
                }

                // Close the accumulo connection.
                try {
                    query.closeConnection(connectionFactory);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }

                String queryId = settings.getId().toString();

                // Stop counting the query towards query limits.
                if (query.getLogic().isQueryLimiterEnabled()) {
                    try {
                        queryLimiter.stopCountingQueryTowardsLimits(queryId);
                    } catch (Exception e) {
                        log.error("Error stopping heartbeat and removing from cache: " + queryId, e);
                    }
                }

                // Remove the query from the query cache.
                queryCache.remove(queryId);

                count++;
                if (log.isDebugEnabled()) {
                    log.debug("Entry evicted, connection returned.");
                }
            }
        }
        if (count > 0 && log.isDebugEnabled()) {
            log.debug(count + " entries evicted from query cache.");
        }
    }

    /**
     * Method to determine if a query has been idle too long based on configured values.
     *
     * @param query
     *            a query
     * @param currentTime
     *            the current time
     * @return true if query has been idle too long, false otherwise
     */
    private boolean isIdleTooLong(RunningQuery query, long currentTime) {
        // not idle if in active call
        if (query.hasActiveCall()) {
            return false;
        }

        long difference = currentTime - query.getLastUsed();
        if (log.isDebugEnabled()) {
            long countDown = (config.getIdleTimeoutMillis() / 1000) - (difference / 1000);
            log.debug("Query: " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " will be evicted in: " + countDown + " seconds.");
        }

        return difference > config.getIdleTimeoutMillis();
    }

    /**
     * Method to determine if a query next call has been running too long based on configured values.
     *
     * @param query
     *            a query
     * @param currentTime
     *            the current time
     * @return true if query next has been running too long, false otherwise
     */
    private boolean isNextTooLong(RunningQuery query, long currentTime) {
        // not in active call then no problem
        if (!query.hasActiveCall()) {
            return false;
        }

        long timeOfCurrentCall = query.getTimeOfCurrentCall();
        int pageCount = query.getCurrentPageCount();

        // if the current call time is 0 but active, then no worries
        if (timeOfCurrentCall == 0) {
            log.warn("Query has active call set but a call time of 0ms.");
            return false;
        }

        query.touch(); // Since we know we're still in a call, go ahead and reset the idle time.
        long difference = currentTime - timeOfCurrentCall;

        // if we are past the short circuit time for page (should be a bit less than the call timeout) and we have results
        // then we need to log where we are potentially stuck, and attempt to trigger a page return
        if ((difference > config.getShortCircuitTimeoutMillis() || difference > query.getTiming().getPageShortCircuitTimeoutMs()) && (pageCount > 0)) {

            // adding quick checks for a potential issue with the page timeout override mechanism
            if (query.getTiming().getPageShortCircuitTimeoutMs() > config.getShortCircuitTimeoutMillis()) {
                log.error("Detected that the running query short circuit timing is configured incorrectly: " + query.getTiming().getPageShortCircuitTimeoutMs()
                                + " > " + config.getShortCircuitTimeoutMillis());
            }
            if (config.getShortCircuitTimeoutMillis() > config.getCallTimeoutMillis()) {
                log.error("Detected that the query short timing is configured incorrectly: " + config.getShortCircuitTimeoutMillis() + " > "
                                + config.getCallTimeoutMillis());
            }

            // log where the next call is currently
            try {
                Exception exception = new Exception("RunningQuery may have been stuck here");
                if (query.getCurrentThread() != null) {
                    exception.setStackTrace(query.getCurrentThread().getStackTrace());
                }
                log.error("Query " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " has been in a call for " + (difference / 1000)
                                + "s and has " + pageCount + " pending results.", exception);
            } catch (Exception e) {
                log.error("Query " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " has been in a call for " + (difference / 1000)
                                + "s and has " + pageCount + " pending results.", e);
            }

            query.attemptForcedPageReturn();
        }

        if (difference > config.getCallTimeoutMillis()) {
            log.warn("Query " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " has been in a call for " + (difference / 1000)
                            + "s.  We are evicting this query from the cache.");
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Query " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " has been in a call for " + (difference / 1000)
                                + "s.");
            }
            return false;
        }
    }

    /**
     * Ensure that the query heartbeat cache within the {@link QueryLimiter} does not consider to be active any queries that are either non-existent or no
     * longer running according to the {@link QueryCache}. This will help safeguard against any synchronization issues.
     */
    private void synchronizeActiveQueries() {
        // Fetch the set of queries considered active by the query limiter.
        Set<String> queryIds = queryLimiter.getActiveQueries();
        Set<String> outdatedQueries = new HashSet<>();

        // Identify any queryIds that do not reference an actively running query.
        for (String queryId : queryIds) {
            RunningQuery query = queryCache.get(queryId);
            if (query != null) {
                if (query.isFinished() || query.isCanceled()) {
                    outdatedQueries.add(queryId);
                }
            } else {
                outdatedQueries.add(queryId);
            }
        }

        // Invalidate the outdated query IDs in the query limiter.
        if (!outdatedQueries.isEmpty()) {
            queryLimiter.stopCountingQueriesTowardsLimits(outdatedQueries);
        }
    }
}
