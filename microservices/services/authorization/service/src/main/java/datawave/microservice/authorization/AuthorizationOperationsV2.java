package datawave.microservice.authorization;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import datawave.microservice.authorization.config.AuthorizationsListSupplier;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.authorization.user.DatawaveUserDetailsFactory;
import datawave.microservice.security.util.DnUtils;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.UserOperations;

/**
 * Presents the REST operations for the authorization service. This version returns the updated (V2) DatawaveUser
 */
@Service("authOperationsV2")
public class AuthorizationOperationsV2 extends AuthorizationOperationsV1 {
    
    public AuthorizationOperationsV2(JWTTokenHandler tokenHandler, CachedDatawaveUserService cachedDatawaveUserService, ApplicationContext appCtx,
                    BusProperties busProperties, AuthorizationsListSupplier authorizationsListSupplier, DnUtils dnUtils,
                    Set<UserOperations> registeredFederatedUserOperations, DatawaveUserDetailsFactory datawaveUserDetailsFactory) {
        super(tokenHandler, cachedDatawaveUserService, appCtx, busProperties, authorizationsListSupplier, dnUtils, registeredFederatedUserOperations,
                        datawaveUserDetailsFactory);
    }
    
    // If there are any proxied users, exclude the last caller from the returned DatawaveUserDetails
    // If there is only one user, return the provided DatawaveUserDetails unchanged
    private DatawaveUserDetails transformCurrentUser(DatawaveUserDetails currentUser) {
        int numUsers = currentUser.getProxiedUsers().size();
        if (numUsers == 1) {
            return currentUser;
        } else {
            return new DatawaveUserDetails(currentUser.getProxiedUsers().stream().limit(numUsers - 1).collect(Collectors.toList()),
                            currentUser.getCreationTime());
        }
    }
    
    public String user(DatawaveUserDetails currentUser) {
        DatawaveUserDetails transformedUser = transformCurrentUser(currentUser);
        return tokenHandler.createTokenFromUsers(transformedUser.getUsername(), transformedUser.getProxiedUsers());
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    public DatawaveUserDetails hello(DatawaveUserDetails currentUser) {
        return transformCurrentUser(currentUser);
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
        return cachedDatawaveUserService.list(username);
    }
}
