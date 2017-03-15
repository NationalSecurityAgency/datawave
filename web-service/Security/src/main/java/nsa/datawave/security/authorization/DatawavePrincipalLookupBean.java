package nsa.datawave.security.authorization;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBContext;
import javax.ejb.Local;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.ejb.TransactionAttribute;
import javax.ejb.TransactionAttributeType;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.inject.Inject;

import com.codahale.metrics.Counter;
import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;
import nsa.datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import nsa.datawave.security.cache.CredentialsCacheBean;
import nsa.datawave.security.cache.PrincipalsCache;
import nsa.datawave.security.util.DnUtils;
import nsa.datawave.webservice.common.cache.SharedCacheCoordinator;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bean retrieves {@link DatawavePrincipal}(s) for either single user/server DNs, or a proxied chain of user/server DNs. As a part of constructing a
 * {@link DatawavePrincipal}, the configured {@link AuthorizationService} is contacted in order to retrieve roles for the user. Those roles are then mapped into
 * Accumulo authorizations using the configured {@link PrincipalFactory}. The user's roles and Accumulo auths are saved with the {@link DatawavePrincipal} and
 * the principal is stored in the principals cache. When multiple DNs are presented, a lookup is performed for each DN and then a combined principal, which
 * contains the roles and auths for all DNs, is returned.
 */
@LocalBean
@Stateless
@RunAs("InternalUser")
@RolesAllowed({"AuthorizedUser", "InternalUser", "AuthorizedServer", "AuthorizedQueryServer", "Administrator"})
@DeclareRoles({"AuthorizedUser", "InternalUser", "AuthorizedServer", "AuthorizedQueryServer", "Administrator"})
@Local(DatawavePrincipalLookup.class)
// Local reference interface
@TransactionManagement(TransactionManagementType.CONTAINER)
@TransactionAttribute(TransactionAttributeType.REQUIRES_NEW)
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class DatawavePrincipalLookupBean implements DatawavePrincipalLookup {
    private Logger log = LoggerFactory.getLogger(getClass());
    
    @Inject
    private PrincipalFactory principalFactory;
    
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Inject
    @PrincipalsCache
    private Cache<String,Principal> credentialsCache;
    
    @Inject
    private DatawavePrincipalLookupConfiguration datawavePrincipalLookupConfig;
    
    @Inject
    private AuthorizationService authorizationService;
    
    @Inject
    private CredentialsCacheBean credentialsCacheManager;
    
    @Inject
    protected SharedCacheCoordinator cacheCoordinator;
    
    @Resource
    private EJBContext context;
    
    @Inject
    @Metric(name = "dw.auth.cacheHits", absolute = true)
    private Counter cacheHits;
    
    @Inject
    @Metric(name = "dw.auth.cacheMisses", absolute = true)
    private Counter cacheMisses;
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.security.authorization.DatawavePrincipalLookup#lookupPrincipal(java.lang.String)
     */
    @Override
    @Timed(name = "dw.auth.lookupPrincipal", absolute = true)
    public DatawavePrincipal lookupPrincipal(final String... userNames) throws Exception {
        return lookupPrincipal(CacheMode.ALL, userNames);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.security.authorization.DatawavePrincipalLookup#lookupPrincipal(java.lang.String)
     */
    @Override
    public DatawavePrincipal lookupPrincipal(final String userName) throws Exception {
        return lookupPrincipal(CacheMode.ALL, userName);
    }
    
    @Override
    public DatawavePrincipal lookupPrincipal(final CacheMode cacheMode, final String... userNames) throws Exception {
        // If there's only a single DN in the list, then delegate
        // delegate directly to the single DN lookup method. Otherwise,
        // look up each DN in the list and merge them all together.
        if (userNames.length == 1) {
            return lookupPrincipal(cacheMode, userNames[0]);
        } else {
            if ((userNames.length % 2) != 0)
                throw new IllegalArgumentException("Invalid DNs are not a multiple of 2: " + Arrays.toString(userNames));
            
            String alias = DnUtils.buildProxiedDN(userNames);
            
            // If there are only 2 username (subject/issuer), then do the single lookup.
            if (userNames.length == 2) {
                return lookupPrincipal(cacheMode, alias);
            }
            
            // Lookup or create the combined principal
            Callable<DatawavePrincipal> createPrincipal = new Callable<DatawavePrincipal>() {
                @Override
                public DatawavePrincipal call() throws Exception {
                    DatawavePrincipal mergedPrincipal = new DatawavePrincipal(userNames);
                    
                    // Look up each principal based on the subject/issuer DN pair, and merge the roles together.
                    for (int i = 0; i < userNames.length; i += 2) {
                        String subjectDN = userNames[i];
                        String issuerDN = userNames[i + 1];
                        String otherDN = DnUtils.buildProxiedDN(subjectDN, issuerDN);
                        DatawavePrincipal otherPrincipal = lookupPrincipal(cacheMode, otherDN);
                        if (otherPrincipal == null) {
                            log.error("User info for {} could not be retrieved.", subjectDN);
                            throw new Exception("User info for " + subjectDN + " could not be retrieved.");
                        }
                        
                        // Merge the new principal into the combined one. This effectively just
                        // adds in the computed roles and ACCUMULO auths for the new principal.
                        principalFactory.mergePrincipals(mergedPrincipal, otherPrincipal);
                    }
                    
                    return mergedPrincipal;
                }
            };
            DatawavePrincipal dp;
            switch (cacheMode) {
                case ALL:
                    dp = lookupOrCreate(alias, createPrincipal);
                    break;
                default:
                    dp = createPrincipal.call();
                    break;
            }
            
            // If the user is missing any of the roles designated as required, then immediately
            // evict the user from the caches so that next time through, we re-query again.
            List<String> requiredRoles = datawavePrincipalLookupConfig.getRequiredRoles();
            if (!dp.getRoleSets().containsAll(requiredRoles)) {
                log.warn("User {} is missing one or more required roles {}.  Evicting from cache.", dp.getUserDN(), requiredRoles);
                credentialsCacheManager.evict(dp.getUserDN());
            }
            return dp;
        }
    }
    
    private DatawavePrincipal lookupPrincipal(CacheMode cacheMode, final String userName) throws Exception {
        final String[] users = DnUtils.splitProxiedSubjectIssuerDNs(userName);
        if ((users.length % 2) != 0)
            throw new IllegalArgumentException("Invalid DN is not a multiple of 2: " + userName);
        
        // If the string was a proxied DN string like <dn1><issuer1><dn2><issuer2>,
        // then we'll split it into a list of multiple DNs. In that case, we want
        // to call the version of the method that takes a list of DNs, since it
        // handles merging all the results together.
        if (users.length > 2)
            return lookupPrincipal(cacheMode, users);
        
        Callable<DatawavePrincipal> createPrincipal = new Callable<DatawavePrincipal>() {
            @Override
            public DatawavePrincipal call() {
                String subjectDN = users[0];
                String issuerDN = users[1];
                
                log.trace("Retrieving roles for {} using {}", subjectDN, authorizationService.getClass().getName());
                String[] rawRoles = authorizationService.getRoles(datawavePrincipalLookupConfig.getProjectName(), subjectDN, issuerDN);
                if (null != rawRoles && rawRoles.length > 1) {
                    Arrays.sort(rawRoles);
                }
                if (log.isDebugEnabled()) {
                    log.debug("{} returned the following roles for {}: {}", authorizationService.getClass().getName(), userName, Arrays.toString(rawRoles));
                }
                
                DatawavePrincipal datawavePrincipal = principalFactory.createPrincipal(userName, rawRoles);
                
                // limit the auths of each component of the principal to the accumuloUserAuths
                TreeSet<String> mergedAuths = new TreeSet<>();
                for (Entry<String,Collection<String>> entry : datawavePrincipal.getAuthorizationsMap().entrySet()) {
                    TreeSet<String> auths = new TreeSet<>();
                    auths.addAll(entry.getValue());
                    auths.retainAll(credentialsCacheManager.getAccumuloUserAuths());
                    datawavePrincipal.setAuthorizations(entry.getKey(), auths);
                    mergedAuths.addAll(auths);
                }
                log.debug("After intersection with ACCUMULO user authorizations, the final authorizations for {}: {}", userName, mergedAuths);
                return datawavePrincipal;
            }
        };
        // Now look up the single user/issuer DN pair and cache the resulting principal object.
        DatawavePrincipal principal;
        switch (cacheMode) {
            case SINGLE_ENTRIES:
            case ALL:
                principal = lookupOrCreate(userName, createPrincipal);
                break;
            default:
                principal = createPrincipal.call();
                break;
        }
        return principal;
    }
    
    private DatawavePrincipal lookupOrCreate(String key, Callable<DatawavePrincipal> createPrincipal) throws Exception {
        DatawavePrincipal principal = (DatawavePrincipal) credentialsCache.get(key);
        if (principal != null) {
            cacheHits.inc();
            log.debug("Found credentials for {} in the cache: {}", key, principal);
        } else {
            // Obtain a distributed lock on the specified key. We do this to be sure that
            // multiple web servers don't call the auth service for the same DN(s) at the same time.
            // First, check the cache and make sure the key isn't there. We do this because
            // we might find the key already there, or in the Accumulo backing store. If
            // we don't find it, then acquire a distributed lock, and do a double-check in
            // case another server put it in the cache between our check and lock acquisition
            // time. If it's still not there, then finally invoke the original method to
            // do the lookup, and put the value in the cache.
            boolean locked = false;
            InterProcessLock lock = cacheCoordinator.getMutex(key);
            try {
                locked = lock.acquire(2, TimeUnit.MINUTES);
                if (!locked) {
                    log.warn("Unable to acquire lock on {}. May be making duplicate authorization service calls.", key);
                } else {
                    log.trace("Obtained lock on principal lookup cache");
                }
            } catch (Exception e) {
                log.warn("Unable to acquire lock on {}. May be making duplicate authorization service calls.", key);
            }
            try {
                // Double-check pattern (not safe with java synchronization, but safe for ZooKeeper).
                // We do this because a bunch of threads could have all come in for the user at the
                // same time. All will see the null in the cache and attempt to lock. The one that
                // gets the lock will call the auth service and insert the principal. Then the rest
                // will get the lock in turn. They need to find out that the value is now there rather
                // than each calling the authorization service.
                principal = (DatawavePrincipal) credentialsCache.get(key);
                if (principal != null) {
                    cacheHits.inc();
                    log.debug("Found credentials for {} in the cache: {}", key, principal);
                } else {
                    cacheMisses.inc();
                    principal = createPrincipal.call();
                    log.trace("Caching credentials for {}", key);
                    credentialsCache.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(key, principal);
                }
            } finally {
                if (locked) {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        log.warn("Unable to release lock on {}: {}", key, e.getMessage(), e);
                        
                    }
                }
            }
        }
        return principal;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see nsa.datawave.security.authorization.DatawavePrincipalLookup#getCurrentPrincipal()
     */
    @Override
    public DatawavePrincipal getCurrentPrincipal() {
        
        if (context == null) {
            return null;
        } else {
            Principal p = context.getCallerPrincipal();
            if (p instanceof DatawavePrincipal) {
                log.info("PRINCIPAL: {}", p.getName());
                return (DatawavePrincipal) p;
            } else {
                log.info("PRINCIPAL: {}", p.getName());
                return null;
            }
        }
    }
    
    @PostConstruct
    protected void postConstruct() {
        if (datawavePrincipalLookupConfig == null)
            throw new IllegalArgumentException("DatawavePrincipalLookupConfiguration not found in JNDI under java:/authorization/datawavePrincipalLookupConfig");
        if (datawavePrincipalLookupConfig.getProjectName() == null)
            throw new IllegalArgumentException("DatawavePrincipalLookupConfiguration must define projectName");
    }
}
