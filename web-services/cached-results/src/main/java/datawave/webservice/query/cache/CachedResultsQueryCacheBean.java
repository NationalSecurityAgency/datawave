package datawave.webservice.query.cache;

import jakarta.annotation.security.DeclareRoles;
import jakarta.annotation.security.PermitAll;
import jakarta.annotation.security.RolesAllowed;
import jakarta.annotation.security.RunAs;
import jakarta.ejb.Lock;
import jakarta.ejb.LockType;
import jakarta.ejb.Singleton;
import jakarta.ejb.Startup;
import jakarta.inject.Inject;

import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;

import datawave.webservice.results.cached.CachedResultsBean;
import datawave.webservice.results.cached.CachedRunningQuery;

@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@DeclareRoles({"AuthorizedUser", "AuthorizedQueryServer", "InternalUser", "Administrator"})
@Startup
// tells the container to initialize on startup
@Singleton
// this is a singleton bean in the container
@Lock(LockType.READ)
// by default all methods are non-blocking
@MBean
public class CachedResultsQueryCacheBean {

    @Inject
    private CachedResultsBean bean;

    @Inject
    private CachedResultsQueryCache cachedRunningQueryCache;

    @PermitAll
    @JmxManaged
    public String listRunningQueries() {
        StringBuilder buf = new StringBuilder();
        // Iterate over the cache contents
        for (CachedRunningQuery crq : cachedRunningQueryCache) {
            buf.append("Identifier: ").append(crq.getQueryId()).append(" Query: ").append(crq).append("\n");
        }
        return buf.toString();
    }

    @JmxManaged
    public String cancelLoad(String queryId) throws Exception {
        try {
            bean.cancelLoad(queryId);
            return "load cancelled.";
        } catch (Exception e) {
            return "Error cancelling query: " + e.getMessage();
        }
    }

}
