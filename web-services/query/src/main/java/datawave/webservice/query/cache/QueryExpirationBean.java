package datawave.webservice.query.cache;

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

import datawave.configuration.spring.SpringBean;
import datawave.microservice.query.Query;
import datawave.microservice.querymetric.BaseQueryMetric;
import org.apache.log4j.Logger;

import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.microservice.query.config.QueryExpirationProperties;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.query.limit.QueryLimiter;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;

import java.util.HashSet;
import java.util.Set;

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

    private QueryCache queryCache;
    private QueryExpirationProperties config;
    private AccumuloConnectionFactory connectionFactory;
    private CreatedQueryLogicCacheBean queryLogicCacheBean;
    private QueryMetricsBean metricsBean;
    private QueryLimiter queryLimiter;

    private boolean clearAll = false;
    
    @Inject
    public void setQueryCache(QueryCache cache) {
        this.queryCache = cache;
    }
    
    @Inject
    @SpringBean(refreshable = true)
    public void setQueryExpirationProperties(QueryExpirationProperties properties) {
        this.config = properties;
    }
    
    @Inject
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }
    
    @Inject
    public void setCreatedQueryLogicCacheBean(CreatedQueryLogicCacheBean cacheBean) {
        this.queryLogicCacheBean = cacheBean;
    }
    
    @Inject
    public void setQueryMetricsBean(QueryMetricsBean metrics) {
        this.metricsBean = metrics;
    }

    @Inject
    public void setQueryLimiter(QueryLimiter queryLimiter) {
        this.queryLimiter = queryLimiter;
    }
    
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
            boolean idleTooLong = !clearAll && !query.hasActiveCall() && isIdleTooLong(query, now);
            boolean nextTooLong = !clearAll && query.hasActiveCall() && isNextTooLong(query, now);
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
                        settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(),
                                        new QueryException(DatawaveErrorCode.SERVER_SHUTDOWN));
                    } else {
                        // If the query has been idle for too long or is taking too long to complete its next call, mark the query as timed out.
                        if (!queryMetric.isLifecycleFinal() && !query.isFinished()) {
                            if(idleTooLong) {
                                queryMetric.setLifecycle(BaseQueryMetric.Lifecycle.TIMEOUT);
                            } else if(nextTooLong) {
                                queryMetric.setLifecycle(BaseQueryMetric.Lifecycle.TIMEOUT);
                            }
                        }
                        settings.getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(),
                                        new QueryException(DatawaveErrorCode.QUERY_TIMEOUT));
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

                // Stop counting the query towards query limits.
                String queryId = settings.getId().toString();
                try {
                    queryLimiter.stopCountingQueryTowardsLimits(queryId);
                } catch (Exception e) {
                    log.error("Error stopping heartbeat and removing from cache: " + queryId, e);
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
        if (query.getTimeOfCurrentCall() == 0) {
            log.warn("Query has active call set but a call time of 0ms.");
            return false;
        }

        query.touch(); // Since we know we're still in a call, go ahead and reset the idle time.
        long difference = currentTime - query.getTimeOfCurrentCall();
        
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
        for(String queryId : queryIds) {
            RunningQuery query = queryCache.get(queryId);
            if(query != null) {
                if(query.isFinished() || query.isCanceled()) {
                    outdatedQueries.add(queryId);
                }
            } else {
                outdatedQueries.add(queryId);
            }
        }
        
        // Invalidate the outdated query IDs in the query limiter.
        if(!outdatedQueries.isEmpty()) {
            queryLimiter.stopCountingQueriesTowardsLimits(outdatedQueries);
        }
    }
}
