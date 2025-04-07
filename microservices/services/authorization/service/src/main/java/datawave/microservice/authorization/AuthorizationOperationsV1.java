package datawave.microservice.authorization;

import static org.springframework.web.accept.ContentNegotiationStrategy.MEDIA_TYPE_ALL_LIST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.AuthorizationEvictionEvent;
import org.springframework.cloud.bus.event.AuthorizationEvictionEvent.Type;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestHeader;

import datawave.microservice.authorization.config.AuthorizationsListSupplier;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.user.DatawaveUserDetailsFactory;
import datawave.microservice.authorization.util.AuthorizationsUtil;
import datawave.microservice.security.util.DnUtils;
import datawave.security.DnList;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.security.authorization.DatawaveUserV1;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;

/**
 * Presents the REST operations for the authorization service. This version returns a DatawaveUserV1 individually and when encapsulated by a DatawaveUserDetails
 * to avoid serialization errors in clients that have not been updated
 */
@Service("authOperationsV1")
public class AuthorizationOperationsV1 {
    private final Logger log = LoggerFactory.getLogger(AuthorizationOperationsV1.class);
    
    protected final JWTTokenHandler tokenHandler;
    protected final CachedDatawaveUserService cachedDatawaveUserService;
    protected final ApplicationContext appCtx;
    protected final BusProperties busProperties;
    
    protected final AuthorizationsListSupplier authorizationsListSupplier;
    
    protected final DnUtils dnUtils;
    
    protected final Set<UserOperations> registeredFederatedUserOperations;
    
    protected final DatawaveUserDetailsFactory datawaveUserDetailsFactory;
    
    public AuthorizationOperationsV1(JWTTokenHandler tokenHandler, CachedDatawaveUserService cachedDatawaveUserService, ApplicationContext appCtx,
                    BusProperties busProperties, AuthorizationsListSupplier authorizationsListSupplier, DnUtils dnUtils,
                    Set<UserOperations> registeredFederatedUserOperations, DatawaveUserDetailsFactory datawaveUserDetailsFactory) {
        this.tokenHandler = tokenHandler;
        this.cachedDatawaveUserService = cachedDatawaveUserService;
        this.appCtx = appCtx;
        this.busProperties = busProperties;
        this.authorizationsListSupplier = authorizationsListSupplier;
        this.dnUtils = dnUtils;
        this.registeredFederatedUserOperations = registeredFederatedUserOperations;
        this.datawaveUserDetailsFactory = datawaveUserDetailsFactory;
    }
    
    // Convert default DatawaveUser (v2) to DatawaveUserV1 for backward compatability of v1 operation
    // If there are any proxied users, exclude the last caller from the returned DatawaveUserDetails
    // If there is only one user, use that user in the returned DatawaveUserDetails
    private DatawaveUserDetails transformCurrentUser(DatawaveUserDetails currentUser) {
        int numUsers = currentUser.getProxiedUsers().size();
        long limit = numUsers == 1 ? 1 : numUsers - 1;
        List<DatawaveUser> proxiedUsersV1 = currentUser.getProxiedUsers().stream().limit(limit).map(u -> new DatawaveUserV1(u)).collect(Collectors.toList());
        return new DatawaveUserDetails(proxiedUsersV1, currentUser.getCreationTime());
    }
    
    public String user(DatawaveUserDetails currentUser) {
        DatawaveUserDetails transformedUser = transformCurrentUser(currentUser);
        return tokenHandler.createTokenFromUsers(transformedUser.getUsername(), transformedUser.getProxiedUsers());
    }
    
    public AuthorizationsListBase<?> listEffectiveAuthorizations(DatawaveUserDetails currentUser, boolean federate) {
        final AuthorizationsListBase<?> list = authorizationsListSupplier.get();
        
        // Find out who/what called this method
        String name = dnUtils.getShortName(currentUser.getPrimaryUser().getName());
        
        if (federate) {
            for (UserOperations federatedUserOperations : registeredFederatedUserOperations) {
                try {
                    DatawaveUserDetails federatedUserDetails = federatedUserOperations.getRemoteUser(currentUser);
                    currentUser = AuthorizationsUtil.mergeProxiedUserDetails(currentUser, federatedUserDetails);
                } catch (Exception e) {
                    log.error("Failed to lookup users from remote user service", e);
                    list.addMessage("Failed to lookup user from remote service: " + e.getMessage());
                }
            }
        }
        
        // Add the user DN's auths into the authorization list
        DatawaveUser primaryUser = currentUser.getPrimaryUser();
        list.setUserAuths(primaryUser.getDn().subjectDN(), primaryUser.getDn().issuerDN(), new HashSet<>(primaryUser.getAuths()));
        
        // Now add all entity auth sets into the list
        currentUser.getProxiedUsers().forEach(u -> list.addAuths(u.getDn().subjectDN(), u.getDn().issuerDN(), new HashSet<>(u.getAuths())));
        
        // Add the role to authorization mapping.
        // NOTE: Currently this is only added for the primary user, which is really all anyone should care about in terms of mucking with
        // authorizations. When used for queries, all non-primary users have all of their auths included -- there is no downgrading.
        list.setAuthMapping(currentUser.getPrimaryUser().getRoleToAuthMapping().asMap());
        log.trace(name + " has authorizations union " + list.getAllAuths());
        return list;
    }
    
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails currentUser, boolean federate) {
        GenericResponse<String> response = new GenericResponse<>();
        log.info("Flushing credentials for " + currentUser + " from the cache.");
        
        // if we have any remote services configured, then flush those credentials as well
        if (federate) {
            for (UserOperations federatedUserOperations : registeredFederatedUserOperations) {
                try {
                    federatedUserOperations.flushCachedCredentials(currentUser);
                } catch (Exception e) {
                    log.error("Failed to flush user from remote user service", e);
                    response.addMessage("Unable to flush user from remote user service");
                    response.addException(e);
                }
            }
        }
        
        response.setResult(evictUser(currentUser.getPrimaryUser().getDn().subjectDN()));
        
        return response;
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    public DatawaveUserDetails hello(DatawaveUserDetails currentUser) {
        return transformCurrentUser(currentUser);
    }
    
    /**
     * Evicts the user identified by the {@link DatawaveUser#getName()} of username from the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @param username
     *            the name of the user to evict
     * @return status indicating whether or not any users were evicted from the authentication cache
     * @see CachedDatawaveUserService#evict(String)
     */
    public String evictUser(String username) {
        appCtx.publishEvent(new AuthorizationEvictionEvent(this, busProperties.getId(), Type.USER, username));
        return cachedDatawaveUserService.evict(username);
    }
    
    /**
     * Evicts all users whose name ({@link DatawaveUser#getName()}) contains the supplied substring from the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return status indicating whether or not any users were evicted from the authentication cache
     * @see CachedDatawaveUserService#evictMatching(String)
     */
    public String evictUsersMatching(String substring) {
        appCtx.publishEvent(new AuthorizationEvictionEvent(this, busProperties.getId(), Type.PARTIAL, substring));
        return cachedDatawaveUserService.evictMatching(substring);
    }
    
    /**
     * Evicts all users from the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return status indicating whether or not any users were evicted from the authentication cache
     * @see CachedDatawaveUserService#evictAll()
     */
    public String evictAll() {
        appCtx.publishEvent(new AuthorizationEvictionEvent(this, busProperties.getId(), Type.FULL, null));
        return cachedDatawaveUserService.evictAll();
    }
    
    /**
     * Lists the user, if any, contained in the authentication cache and having a {@link DatawaveUser#getName()} of name.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @param username
     *            the name of the user to list
     * @return the cached user whose {@link DatawaveUser#getName()} is name, or null if no such user is cached
     * @see CachedDatawaveUserService#list(String)
     */
    public DatawaveUser listCachedUser(String username) {
        DatawaveUser user = cachedDatawaveUserService.list(username);
        return user == null ? null : new DatawaveUserV1(user);
    }
    
    /**
     * Lists the users, if any, contained in the authentication cache and containing substring in their {@link DatawaveUser#getName()}.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @param substring
     *            the sub-string to be contained in all returned users' {@link DatawaveUser#getName()}
     * @return the matching cached users, ifany
     * @see CachedDatawaveUserService#listMatching(String)
     */
    public Collection<? extends DatawaveUserInfo> listCachedUsersMatching(String substring) {
        return cachedDatawaveUserService.listMatching(substring);
    }
    
    /**
     * Lists all users stored in the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return a collection of all {@link DatawaveUser}s that are stored in the authentication cache
     * @see CachedDatawaveUserService#listAll()
     */
    public Object listCachedUsers(@RequestHeader HttpHeaders headers) {
        Collection<? extends DatawaveUserInfo> dnList = cachedDatawaveUserService.listAll();
        
        MediaType preferredMediaType = determinePreferredMediaType(headers.getAccept());
        if (preferredMediaType.equals(MediaType.TEXT_HTML)) {
            return new DnList(dnList);
        } else {
            return dnList;
        }
    }
    
    private MediaType determinePreferredMediaType(List<MediaType> mediaTypes) {
        mediaTypes = !CollectionUtils.isEmpty(mediaTypes) ? mediaTypes : MEDIA_TYPE_ALL_LIST;
        MediaType.sortBySpecificityAndQuality(mediaTypes);
        return mediaTypes.get(0);
    }
}
