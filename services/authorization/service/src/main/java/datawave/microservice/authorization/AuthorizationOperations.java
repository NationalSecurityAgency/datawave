package datawave.microservice.authorization;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.webservice.security.JWTTokenHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.security.RolesAllowed;
import java.util.Collection;

/**
 * Presents the REST operations for the authorization service.
 */
@RestController
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE)
public class AuthorizationOperations {
    private final JWTTokenHandler tokenHandler;
    private final CachedDatawaveUserService cachedDatawaveUserService;
    
    @Autowired
    public AuthorizationOperations(JWTTokenHandler tokenHandler, CachedDatawaveUserService cachedDatawaveUserService) {
        this.tokenHandler = tokenHandler;
        this.cachedDatawaveUserService = cachedDatawaveUserService;
    }
    
    @RequestMapping(path = "/authorize", produces = MediaType.TEXT_PLAIN_VALUE, method = RequestMethod.GET)
    public String user(@AuthenticationPrincipal ProxiedUserDetails currentUser) {
        return tokenHandler.createTokenFromUsers(currentUser.getUsername(), currentUser.getProxiedUsers());
    }
    
    /**
     * Returns the {@link ProxiedUserDetails} that represents the authenticated calling user.
     */
    @RequestMapping(path = "/whoami", method = RequestMethod.GET)
    public ProxiedUserDetails hello(@AuthenticationPrincipal ProxiedUserDetails currentUser) {
        return currentUser;
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictUser", produces = MediaType.TEXT_PLAIN_VALUE, method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictUser(@RequestParam String username) {
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictUsersMatching", produces = MediaType.TEXT_PLAIN_VALUE, method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictUsersMatching(@RequestParam String substring) {
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictAll", produces = MediaType.TEXT_PLAIN_VALUE, method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictAll() {
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUser", method = RequestMethod.GET)
    public DatawaveUser listCachedUser(@RequestParam String username) {
        return cachedDatawaveUserService.list(username);
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUsersMatching", method = RequestMethod.GET)
    public Collection<? extends DatawaveUser> listCachedUsersMatching(@RequestParam String substring) {
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
    @RolesAllowed({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUsers", method = RequestMethod.GET)
    public Collection<? extends DatawaveUser> listCachedUsers() {
        return cachedDatawaveUserService.listAll();
    }
}
