package datawave.microservice.authorization;

import static datawave.microservice.http.converter.protostuff.ProtostuffHttpMessageConverter.PROTOSTUFF_VALUE;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.DatawaveUserInfo;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * Presents the REST operations for the authorization service. This version returns a DatawaveUserV1 individually and when encapsulated by a DatawaveUserDetails
 * to avoid serialization errors in clients that have not been updated
 */
@Tag(name = "Authorization Operations /v1",
                externalDocs = @ExternalDocumentation(description = "Authorization Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-authorization-service"))
@RestController
@RequestMapping(path = "/v1", produces = MediaType.APPLICATION_JSON_VALUE)
public class AuthorizationControllerV1 {
    private final AuthorizationOperationsV1 authOperations;
    
    @Autowired
    public AuthorizationControllerV1(@Qualifier("authOperationsV1") AuthorizationOperationsV1 authOperations) {
        this.authOperations = authOperations;
    }
    
    @Operation(summary = "Returns a JWT of the current user/proxied user(s)",
                    description = "The returned JWT can be passed to other calls in a header. For example: \"Authorization: Bearer <JWT value>\".<br>>"
                                    + "The JWT is created from the proxied users if present or from the supplied client certificate "
                                    + "or trusted headers (X-SSL-clientcert-subject/X-SSL-clientcert-issuer) if there are no proxied users.")
    @RequestMapping(path = "/authorize", produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE}, method = RequestMethod.GET)
    public String user(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return authOperations.user(currentUser);
    }
    
    @Operation(summary = "Lists the effective Accumulo user authorizations for the calling user.")
    @RequestMapping(path = "/listEffectiveAuthorizations", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE,
            MediaType.APPLICATION_XML_VALUE, MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE, MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
    public AuthorizationsListBase<?> listEffectiveAuthorizations(
                    @Parameter(description = "Whether the request should be federated to downstream services.") @RequestParam(name = "includeRemoteServices",
                                    defaultValue = "true", required = false) boolean federate,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return authOperations.listEffectiveAuthorizations(currentUser, federate);
    }
    
    @Operation(summary = "Clears any cached credentials for the calling user.")
    @RequestMapping(path = "/flushCachedCredentials", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE,
            MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE, MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
    public GenericResponse<String> flushCachedCredentials(
                    @Parameter(description = "Whether the request should be federated to downstream services.") @RequestParam(
                                    name = "includeRemoteServices") boolean federate,
                    @AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return authOperations.flushCachedCredentials(currentUser, federate);
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    @Operation(summary = "Returns details about the current user/proxied user(s).",
                    description = "The user(s) can be determined from the proxied user(s) if present or from the supplied client certificate "
                                    + "or trusted headers (X-SSL-clientcert-subject/X-SSL-clientcert-issuer) if there are no proxied users.")
    @RequestMapping(path = "/whoami", method = RequestMethod.GET)
    public DatawaveUserDetails hello(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return authOperations.hello(currentUser);
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
    @Operation(summary = "Evicts the named user from the authorization cache.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictUser", produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
                    method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictUser(@Parameter(description = "The username (e.g., subjectDn<issuerDn>) to evict") @RequestParam String username) {
        return authOperations.evictUser(username);
    }
    
    /**
     * Evicts all users whose name ({@link DatawaveUser#getName()}) contains the supplied substring from the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return status indicating whether or not any users were evicted from the authentication cache
     * @see CachedDatawaveUserService#evictMatching(String)
     */
    @Operation(summary = "Evicts from the authorization cache all users whose name contains the supplied substring.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictUsersMatching", produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
                    method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictUsersMatching(@Parameter(description = "A substring to search for in user names to evict") @RequestParam String substring) {
        return authOperations.evictUsersMatching(substring);
    }
    
    /**
     * Evicts all users from the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return status indicating whether or not any users were evicted from the authentication cache
     * @see CachedDatawaveUserService#evictAll()
     */
    @Operation(summary = "Evicts all users from the authorization cache.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/evictAll", produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
                    method = {RequestMethod.GET, RequestMethod.DELETE})
    public String evictAll() {
        return authOperations.evictAll();
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
    @Operation(summary = "Lists the details for the named cached user.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUser", method = RequestMethod.GET)
    public DatawaveUser listCachedUser(@Parameter(description = "The username (e.g., subjectDn<issuerDn>) to evict") @RequestParam String username) {
        return authOperations.listCachedUser(username);
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
    @Operation(summary = "Retrieves details for all cached users whose names match a substring.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUsersMatching", method = RequestMethod.GET)
    public Collection<? extends DatawaveUserInfo> listCachedUsersMatching(
                    @Parameter(description = "A substring to search for in user names to list") @RequestParam String substring) {
        return authOperations.listCachedUsersMatching(substring);
    }
    
    /**
     * Lists all users stored in the authentication cache.
     * <p>
     * Note that access to this method is restricted to those users with administrative credentials.
     *
     * @return a collection of all {@link DatawaveUser}s that are stored in the authentication cache
     * @see CachedDatawaveUserService#listAll()
     */
    @Operation(summary = "Retrieves details for all cached users.")
    @Secured({"Administrator", "JBossAdministrator"})
    @RequestMapping(path = "/admin/listUsers", method = RequestMethod.GET, produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE,
            MediaType.TEXT_XML_VALUE, PROTOSTUFF_VALUE, MediaType.TEXT_HTML_VALUE, "text/x-yaml", "application/x-yaml"})
    public Object listCachedUsers(@RequestHeader HttpHeaders headers) {
        return authOperations.listCachedUsers(headers);
    }
}
