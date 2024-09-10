package datawave.webservice.query.cache;

import java.util.Map;

import javax.annotation.PreDestroy;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;

import org.apache.log4j.Logger;

import datawave.configuration.spring.SpringBean;
import datawave.webservice.results.cached.CachedResultsBean;
import datawave.webservice.results.cached.CachedRunningQuery;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.WRITE)
// by default all methods are blocking
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
@TransactionManagement(TransactionManagementType.BEAN)
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
        for (Map.Entry<String,CachedRunningQuery> entry : cachedRunningQueryCache.entrySet()) {
            CachedRunningQuery crq = entry.getValue();
            String cacheId = entry.getKey();
            crq.closeConnection(log);
            cachedRunningQueryCache.remove(cacheId);
            // cancel any concurrent load
            String originalQuery = crq.getOriginalQueryId();
            if (crb.isQueryLoading(originalQuery)) {
                try {
                    crb.cancelLoadByAdmin(originalQuery);
                } catch (Exception e) {
                    // Do nothing
                }
            }
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
        for (Map.Entry<String,CachedRunningQuery> entry : cachedRunningQueryCache.entrySet()) {
            CachedRunningQuery crq = entry.getValue();
            String cacheId = entry.getKey();
            long difference = now - crq.getLastUsed();
            if (log.isTraceEnabled()) {
                log.trace("key: " + cacheId + ", now: " + now + ", last used: " + crq.getLastUsed() + " difference: " + difference);
            }
            if ((difference > cachedResultsExpirationConfiguration.getEvictionTimeMs())) {
                crq.closeConnection(log);
                closeCount++;
                evictionCount++;
                cachedRunningQueryCache.remove(cacheId);
                // cancel any concurrent load
                String originalQueryId = crq.getOriginalQueryId();
                if (crb.isQueryLoading(originalQueryId)) {
                    try {
                        crb.cancelLoadByAdmin(originalQueryId);
                    } catch (Exception e) {
                        // Do nothing
                    }
                }
                log.debug("CachedRunningQuery " + cacheId + " connections returned and removed from cache");
            } else if ((difference > cachedResultsExpirationConfiguration.getCloseConnectionsTimeMs())) {
                crq.closeConnection(log);
                closeCount++;
                log.debug("CachedRunningQuery " + cacheId + " connections returned");
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
