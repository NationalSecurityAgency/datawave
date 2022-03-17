package datawave.webservice.query.cache;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.SpringBean;
import datawave.webservice.common.connection.AccumuloConnectionFactory;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import datawave.microservice.querymetric.QueryMetric;
import datawave.webservice.query.metric.QueryMetricsBean;
import datawave.webservice.query.runner.RunningQuery;
import datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

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
import java.util.Date;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
@Singleton
@DependsOn({"QueryMetricsBean", "AccumuloConnectionFactoryBean"})
@Lock(LockType.WRITE)
// by default all methods are blocking
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class QueryExpirationBean {
    
    private static final Logger log = Logger.getLogger(QueryExpirationBean.class);
    
    @Inject
    private QueryCache cache;
    
    @Inject
    @SpringBean(refreshable = true)
    private QueryExpirationConfiguration conf;
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    
    @Inject
    private CreatedQueryLogicCacheBean qlCache;
    
    @Inject
    private QueryMetricsBean metrics;
    
    private boolean clearAll = false;
    
    @PostConstruct
    public void init() {
        if (log.isDebugEnabled()) {
            log.debug("@PostConstruct - init()");
        }
        
        if (conf == null) {
            throw new IllegalArgumentException("QueryExpirationConfiguration is null");
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
        qlCache.clearQueryLogics(now, conf.getCallTimeInMS());
    }
    
    private void clearQueries(long now) {
        int count = 0;
        
        for (RunningQuery query : cache) {
            boolean idleTooLong = !clearAll && !query.hasActiveCall() && isIdleTooLong(query, now);
            boolean nextTooLong = !clearAll && query.hasActiveCall() && isNextTooLong(query, now);
            if (clearAll || idleTooLong || nextTooLong) {
                if (query.getSettings().getUncaughtExceptionHandler() == null) {
                    query.getSettings().setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
                }
                try {
                    if (clearAll) {
                        query.getMetric().setLifecycle(QueryMetric.Lifecycle.SHUTDOWN);
                        query.getSettings().getUncaughtExceptionHandler()
                                        .uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.SERVER_SHUTDOWN));
                    } else {
                        if (!query.getMetric().isLifecycleFinal() && !query.isFinished() && idleTooLong) {
                            query.getMetric().setLifecycle(QueryMetric.Lifecycle.TIMEOUT);
                        }
                        if (!query.getMetric().isLifecycleFinal() && !query.isFinished() && nextTooLong) {
                            query.getMetric().setLifecycle(QueryMetric.Lifecycle.NEXTTIMEOUT);
                        }
                        
                        query.getSettings().getUncaughtExceptionHandler()
                                        .uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.QUERY_TIMEOUT));
                    }
                } finally {
                    if (query.getLogic().getCollectQueryMetrics()) {
                        try {
                            metrics.updateMetric(query.getMetric());
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
                
                if (query.hasActiveCall()) {
                    query.cancel();
                }
                try {
                    query.closeConnection(connectionFactory);
                } catch (Exception e) {
                    log.error("Error returning connection to factory", e);
                }
                cache.remove(query.getSettings().getId().toString());
                count++;
                if (log.isDebugEnabled()) {
                    log.debug("Entry evicted, connection returned.");
                }
                
                TInfo traceInfo = query.getTraceInfo();
                if (traceInfo != null) {
                    Span span = Trace.trace(traceInfo, "query:expiration");
                    span.data("expiredAt", new Date().toString());
                    // Spans aren't recorded if they take no time, so sleep for a
                    // couple milliseconds just to ensure we get something saved.
                    try {
                        Thread.sleep(2);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    span.stop();
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
     * @param currentTime
     * @return true if query has been idle too long, false otherwise
     */
    private boolean isIdleTooLong(RunningQuery query, long currentTime) {
        long difference = currentTime - query.getLastUsed();
        if (log.isDebugEnabled()) {
            long countDown = (conf.getIdleTimeInMS() / 1000) - (difference / 1000);
            log.debug("Query: " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " will be evicted in: " + countDown + " seconds.");
        }
        
        return difference > conf.getIdleTimeInMS();
    }
    
    /**
     * Method to determine if a query next call has been running too long based on configured values.
     *
     * @param query
     * @param currentTime
     * @return true if query next has been running too long, false otherwise
     */
    private boolean isNextTooLong(RunningQuery query, long currentTime) {
        if (query.getTimeOfCurrentCall() == 0) {
            log.warn("Query has active call set but a call time of 0ms.");
            return false;
        }
        
        query.touch(); // Since we know we're still in a call, go ahead and reset the idle time.
        long difference = currentTime - query.getTimeOfCurrentCall();
        
        if (difference > conf.getCallTimeInMS()) {
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
}
