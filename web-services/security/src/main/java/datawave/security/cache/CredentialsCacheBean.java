package datawave.security.cache;

import datawave.configuration.ConfigurationEvent;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.RefreshLifecycle;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.security.DnList;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.system.AuthorizationCache;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.GenericResponse;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.deltaspike.core.api.jmx.JmxManaged;
import org.apache.deltaspike.core.api.jmx.MBean;
import org.jboss.security.CacheableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static datawave.webservice.query.exception.DatawaveErrorCode.UNKNOWN_SERVER_ERROR;

/**
 * A service for managing cached {@link DatawaveUser} objects. It should be noted that there are potentially two caches in use. The first is a general Wildfly
 * cache that caches computed JAAS {@link Principal}s which, in turn, contain references to {@link DatawaveUser}s. The second is an application level cache (see
 * {@link CachedDatawaveUserService}). The methods here will operate on the application-level cache, if available, and also will modify the Wildfly cache.
 */
@Path("/Security/Admin/Credentials")
@LocalBean
// tells the container to initialize on startup
@Startup
// this is a singleton bean in the container
@Singleton
@RunAs("InternalUser")
@RolesAllowed({"JBossAdministrator", "Administrator", "SecurityUser", "InternalUser"})
@DeclareRoles({"JBossAdministrator", "Administrator", "SecurityUser", "InternalUser"})
@MBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
@TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
// transactions not supported directly by this bean
public class CredentialsCacheBean {
    protected Logger log = LoggerFactory.getLogger(getClass());
    
    @Inject
    @AuthorizationCache
    private CacheableManager<?,Principal> authManager;
    
    @Inject
    private Instance<CachedDatawaveUserService> cachedDatawaveUserServiceInstance;
    
    @Inject
    private AccumuloConnectionFactory accumuloConnectionFactory;
    
    private Set<String> accumuloUserAuths = new HashSet<>();
    
    private Exception flushAllException;
    
    @PostConstruct
    protected void postConstruct() {
        try {
            retrieveAccumuloAuthorizations();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Removes all cached {@link DatawaveUser}s. There are potentially two caches in use. First, Wildfly uses a security cache that stores {@link Principal}s
     * under the incoming credential key. This is normally a very short-lived cache (5-30 minutes). Second, a {@link CachedDatawaveUserService} may be in use,
     * which means that it caches according to its own rules. This method attempts to clear both caches.
     *
     * @return a string indicating cache flush was successful
     */
    @GET
    @Path("/flushAll")
    @JmxManaged
    public String flushAll() {
        try {
            // Remove principals from the Wildfly cached authentication manager, if we have one in use.
            authManager.flushCache();
            if (!cachedDatawaveUserServiceInstance.isUnsatisfied()) {
                cachedDatawaveUserServiceInstance.get().evictAll();
            }
            return "All credentials caches cleared.";
        } catch (Exception e) {
            GenericResponse<String> response = new GenericResponse<>();
            response.addException(new QueryException(UNKNOWN_SERVER_ERROR, e));
            throw new DatawaveWebApplicationException(e, response);
        }
    }
    
    /**
     * Evicts {@code dn} from authorization caches. There are potentially two caches in use. First, Wildfly uses a security cache that stores {@link Principal}s
     * under the incoming credential key. This is normally a very short-lived cache (5-30 minutes). Second, a {@link CachedDatawaveUserService} may be in use,
     * which means that it caches according to its own rules. This method attempts to clear both caches.
     *
     * @param dn
     *            the DN to evict from authorization caches
     *            
     * @return a string indicating cache flush was successful
     */
    @GET
    @Path("/{dn}/evict")
    @Produces({"text/plain"})
    @JmxManaged
    public String evict(@PathParam("dn") String dn) {
        
        String result = "Evicted " + dn + " from the credentials cache.";
        if (!cachedDatawaveUserServiceInstance.isUnsatisfied()) {
            result = cachedDatawaveUserServiceInstance.get().evictMatching(dn);
        }
        // @formatter:off
        // Flush all principals from the Wildfly cache if the getName() of any of the contained DatawaveUser objects matches the supplied DN.
        authManager.getCachedKeys().parallelStream()
                .filter(p -> p instanceof DatawavePrincipal)
                .filter(p -> ((DatawavePrincipal) p).getProxiedUsers().stream().anyMatch(u -> u.getName().equals(dn)))
                .forEach(p -> {
                    log.debug("Evicting {} from the Wildfly authentication cache.", p);
                    authManager.flushCache(p);
                });
        // @formatter:on
        return result;
    }
    
    /**
     * List DNs for all DatawaveUser objects stored in the cache.
     *
     * @param localOnly
     *            If true, then only entries from the local Wildfly cache are returned. Otherwise, entries from any available {@link CachedDatawaveUserService}
     *            are returned.
     *
     * @return a {@link DnList} containing DNs for principals stored in the cache.
     */
    @GET
    @Path("/listDNs")
    @Produces({"text/plain", "application/xml", "text/xml", "application/json", "text/html"})
    @JmxManaged
    public DnList listDNs(@QueryParam("localOnly") boolean localOnly) {
        DnList result;
        if (!cachedDatawaveUserServiceInstance.isUnsatisfied() && !localOnly) {
            result = new DnList(cachedDatawaveUserServiceInstance.get().listAll());
        } else {
            // @formatter:off
            Set<DatawaveUserInfo> userList = authManager.getCachedKeys().parallelStream()
                    .filter(p -> p instanceof DatawavePrincipal)
                    .flatMap(p -> ((DatawavePrincipal) p).getProxiedUsers().stream())
                    .map(DatawaveUserInfo::new)
                    .collect(Collectors.toSet());
            // @formatter:on
            result = new DnList(userList);
        }
        return result;
    }
    
    /**
     * Lists all DNs contained in the cache containing the substring {@code substr}.
     *
     * @param substr
     *            the substring to search for in the cache
     * @return a {@link DnList} containing DNs for matching users stored in the cache.
     */
    @GET
    @Path("/listMatching")
    @Produces({"text/plain", "application/xml", "text/xml", "application/json", "text/html"})
    @JmxManaged
    public DnList listDNsMatching(@QueryParam("substring") final String substr) {
        DnList result;
        if (!cachedDatawaveUserServiceInstance.isUnsatisfied()) {
            result = new DnList(cachedDatawaveUserServiceInstance.get().listMatching(substr));
        } else {
            Set<Principal> principals = authManager.getCachedKeys();
            // @formatter:off
            Set<DatawaveUserInfo> userList = principals.parallelStream()
                    .filter(p -> p instanceof DatawavePrincipal)
                    .flatMap(p -> ((DatawavePrincipal) p).getProxiedUsers().stream())
                    .filter(u -> u.getName().contains(substr))
                    .map(DatawaveUserInfo::new)
                    .collect(Collectors.toSet());
            // @formatter:on
            result = new DnList(userList);
        }
        return result;
    }
    
    /**
     * Retrieves the {@link DatawaveUser} for {@code dn} from the users cache.
     *
     * @param dn
     *            the DN of the {@link DatawaveUser} to retrieve from the users cache
     *            
     * @return a string indicating cache flush was successful
     */
    @GET
    @Path("/{dn}/list")
    @Produces({"text/plain", "application/xml", "text/xml", "application/json", "text/html"})
    @JmxManaged
    public DatawaveUser list(@PathParam("dn") String dn) {
        DatawaveUser user;
        if (!cachedDatawaveUserServiceInstance.isUnsatisfied()) {
            user = cachedDatawaveUserServiceInstance.get().list(dn);
        } else {
            // @formatter:off
            user = authManager.getCachedKeys().parallelStream()
                    .filter(p -> p instanceof DatawavePrincipal)
                    .flatMap(p -> ((DatawavePrincipal) p).getProxiedUsers().stream())
                    .filter(p -> p.getName().equals(dn))
                    .findFirst()
                    .orElse(null);
            // @formatter:on
        }
        return user;
    }
    
    @SuppressWarnings("unused")
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    // transactions not supported directly by this bean
    public void onConfigurationUpdate(@Observes ConfigurationEvent event) {
        log.debug("Received a configuration update event. Re-querying Accumulo user authorizations and invalidating users cache.");
        try {
            HashSet<String> oldAccumuloAuths = new HashSet<>(accumuloUserAuths);
            
            if (log.isTraceEnabled()) {
                log.trace("Received refresh event on 0x{}. Retrieving new Accumulo authorizations.", Integer.toHexString(System.identityHashCode(this)));
            }
            retrieveAccumuloAuthorizations();
            
            if (!accumuloUserAuths.equals(oldAccumuloAuths)) {
                // Flush the principals cache (and attempt to tell other web servers to do the same)
                // If there's a problem, however, do not fail the entire refresh event. Do that at the end.
                try {
                    flushAll();
                } catch (Exception e) {
                    flushAllException = e;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    @SuppressWarnings("unused")
    @TransactionAttribute(TransactionAttributeType.NOT_SUPPORTED)
    public void onRefreshComplete(@Observes RefreshLifecycle refreshLifecycle) {
        switch (refreshLifecycle) {
            case INITIATED:
                flushAllException = null;
                break;
            case COMPLETE:
                // Now that the refresh is complete, throw any flush principals exception that might have happened.
                // We want to let the rest of the refresh complete internally before throwing the error so that we
                // don't leave this server in an inconsistent state.
                if (flushAllException != null) {
                    throw new RuntimeException("Error flushing principals cache: " + flushAllException.getMessage(), flushAllException);
                }
                break;
        }
    }
    
    @GET
    @Path("/listAccumuloAuths")
    @Produces({"text/plain", "application/json"})
    @JmxManaged
    public Set<String> getAccumuloUserAuths() {
        return accumuloUserAuths;
    }
    
    @GET
    @Path("/reloadAccumuloAuths")
    @Produces({"application/xml", "text/xml", "application/json"})
    public GenericResponse<String> reloadAccumuloAuthorizations() {
        GenericResponse<String> response = new GenericResponse<>();
        try {
            retrieveAccumuloAuthorizations();
            response.setResult("Authorizations reloaded. Remember to flush the principals cache to ensure principals are reloaded with new auths applied.");
            return response;
        } catch (Exception e) {
            response.setResult("Unable to reload Accumulo authorizations.");
            throw new DatawaveWebApplicationException(e, response);
        }
    }
    
    private void retrieveAccumuloAuthorizations() throws Exception {
        Map<String,String> trackingMap = accumuloConnectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        AccumuloClient c = accumuloConnectionFactory.getClient(null, null, AccumuloConnectionFactory.Priority.ADMIN, trackingMap);
        try {
            Authorizations auths = c.securityOperations().getUserAuthorizations(c.whoami());
            HashSet<String> authSet = new HashSet<>();
            for (byte[] auth : auths.getAuthorizations()) {
                authSet.add(new String(auth).intern());
            }
            accumuloUserAuths = Collections.unmodifiableSet(authSet);
            log.debug("Accumulo User Authorizations: {}", accumuloUserAuths);
        } finally {
            accumuloConnectionFactory.returnClient(c);
        }
    }
}
