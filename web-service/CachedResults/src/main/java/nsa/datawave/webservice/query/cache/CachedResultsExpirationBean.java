package nsa.datawave.webservice.query.cache;

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

import nsa.datawave.configuration.spring.SpringBean;
import nsa.datawave.webservice.results.cached.CachedResultsBean;
import nsa.datawave.webservice.results.cached.CachedRunningQuery;
import org.apache.log4j.Logger;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.WRITE)
// by default all methods are blocking
public class CachedResultsExpirationBean {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Inject
    private CachedResultsQueryCache cachedRunningQueryCache;
    
    // reference datawave/query/CachedResultsExpiration.xml
    @Inject
    @SpringBean(required = false, refreshable = true)
    private CachedResultsExpirationConfiguration cachedResultsExpirationConfiguration;
    
    @Inject
    private CachedResultsBean crb;
    
    @PreDestroy
    public void close() {
        // This is not a pre-destroy hook in CachedResultsBean because many
        // instances of it are created and destroyed by the container, whereas
        // this is a singleton.
        log.debug("in shutdown method");
        for (CachedRunningQuery crq : cachedRunningQueryCache) {
            CachedResultsBean.closeCrqConnection(crq);
            cachedRunningQueryCache.remove(crq.getQueryId());
        }
        log.debug("Shutdown method completed.");
    }
    
    @Schedule(hour = "*", minute = "*", second = "*/30", persistent = false)
    public void removeIdleOrExpired() {
        int closeCount = 0;
        int evictionCount = 0;
        long now = System.currentTimeMillis();
        // The cache eviction notifications are not working.
        // Using an interceptor is not working either.
        // This method will be invoked every minute by the timer service and will
        // evict entries that are idle or expired.
        for (CachedRunningQuery srq : cachedRunningQueryCache) {
            String cachedQueryId = srq.getQueryId();
            long difference = now - srq.getLastUsed();
            if (log.isTraceEnabled()) {
                log.trace("key: " + cachedQueryId + ", now: " + now + ", last used: " + srq.getLastUsed() + " difference: " + difference);
            }
            if ((difference > cachedResultsExpirationConfiguration.getEvictionTimeMs())) {
                CachedResultsBean.closeCrqConnection(srq);
                closeCount++;
                evictionCount++;
                cachedRunningQueryCache.remove(cachedQueryId);
                // cancel any concurrent load
                crb.cancelLoad(cachedQueryId);
                log.debug("CachedRunningQuery " + cachedQueryId + " connections returned and removed from cache");
            } else if ((difference > cachedResultsExpirationConfiguration.getCloseConnectionsTimeMs())) {
                CachedResultsBean.closeCrqConnection(srq);
                closeCount++;
                log.debug("CachedRunningQuery " + cachedQueryId + " connections returned");
            }
            
        }
        if (closeCount > 0) {
            log.debug(closeCount + " entries closed");
        }
        if (evictionCount > 0) {
            log.debug(evictionCount + " entries evicted");
        }
    }
}
