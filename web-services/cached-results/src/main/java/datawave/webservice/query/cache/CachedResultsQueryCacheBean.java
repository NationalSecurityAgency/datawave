package datawave.webservice.query.cache;

import datawave.webservice.results.cached.CachedResultsBean;
import datawave.webservice.results.cached.CachedRunningQuery;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;

import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.Lock;
import javax.ejb.LockType;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;

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
