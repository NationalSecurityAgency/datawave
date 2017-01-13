package nsa.datawave.webservice.query.cache;

import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;
import nsa.datawave.webservice.query.metric.QueryMetric;
import nsa.datawave.webservice.query.runner.RunningQuery;
import nsa.datawave.webservice.query.util.QueryUncaughtExceptionHandler;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
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
    
    /**
     * Method to determine if a query should be evicted from the cache based on configured values.
     */
    private boolean shouldRemove(RunningQuery query, long currentTime) {
        if (!query.hasActiveCall()) {
            long difference = currentTime - query.getLastUsed();
            if (log.isDebugEnabled()) {
                long countDown = (conf.getIdleTimeInMS() / 1000) - (difference / 1000);
                log.debug("Query: " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " will be evicted in: " + countDown + " seconds.");
            }
            
            return difference > conf.getIdleTimeInMS();
        }
        
        if (query.getTimeOfCurrentCall() == 0) {
            log.warn("Query has active call set but a call time of 0ms.");
            return false;
        }
        
        query.touch(); // Since we know we're still in a call, go ahead and reset the idle time.
        long difference = currentTime - query.getTimeOfCurrentCall();
        
        if (log.isDebugEnabled()) {
            log.debug("Query " + query.getSettings().getOwner() + " - " + query.getSettings().getId() + " has been in a call for " + (difference / 1000) + "s.");
        }
        
        if (difference > conf.getCallTimeInMS()) {
            log.warn("A query has had a call running against it for: " + difference + "ms. We are evicting the query from the cache.");
            return true;
        }
        
        return false;
    }
    
    private void clearQueries(long now) {
        int count = 0;
        
        for (RunningQuery query : cache) {
            if (clearAll || shouldRemove(query, now)) {
                
                if (query.getSettings().getUncaughtExceptionHandler() == null) {
                    query.getSettings().setUncaughtExceptionHandler(new QueryUncaughtExceptionHandler());
                }
                if (clearAll) {
                    query.getSettings().getUncaughtExceptionHandler()
                                    .uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.SERVER_SHUTDOWN));
                } else {
                    if (!query.getMetric().isLifecycleFinal() && !query.isFinished() && !query.hasActiveCall() && isIdleTooLong(query, now)) {
                        query.getMetric().setLifecycle(QueryMetric.Lifecycle.TIMEOUT);
                    }
                    if (!query.getMetric().isLifecycleFinal() && !query.isFinished() && query.hasActiveCall() && isNextTooLong(query, now)) {
                        query.getMetric().setLifecycle(QueryMetric.Lifecycle.NEXTTIMEOUT);
                    }
                    
                    query.getSettings().getUncaughtExceptionHandler()
                                    .uncaughtException(Thread.currentThread(), new QueryException(DatawaveErrorCode.QUERY_TIMEOUT));
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
        long difference = currentTime - query.getLastUsed();
        
        return difference > query.getTimeOfCurrentCall();
    }
}
