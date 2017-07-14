package datawave.security.user;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Resource;
import javax.annotation.security.DeclareRoles;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RolesAllowed;
import javax.annotation.security.RunAs;
import javax.ejb.EJBContext;
import javax.ejb.LocalBean;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.security.authorization.AuthorizationService;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.PrincipalFactory;
import datawave.security.cache.CredentialsCacheBean;
import datawave.security.util.DnUtils;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

@Path("/Security/User")
@LocalBean
@Stateless
@RunAs("InternalUser")
@RolesAllowed({"InternalUser", "AuthorizedUser", "AuthorizedServer", "AuthorizedQueryServer", "SecurityUser"})
@DeclareRoles({"InternalUser", "AuthorizedUser", "AuthorizedServer", "AuthorizedQueryServer", "SecurityUser"})
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class UserOperationsBean {
    private Logger log = Logger.getLogger(getClass());
    
    @Resource
    private EJBContext context;
    
    @Inject
    private PrincipalFactory principalFactory;
    
    @Inject
    private AuthorizationService authorizationService;
    
    @Inject
    private CredentialsCacheBean credentialsCacheService;
    
    @Inject
    private ResponseObjectFactory responseObjectFactory;
    
    /**
     * Lists the "effective" Accumulo user authorizations for the calling user. These are authorizations that are returned by the authorization service
     * (possibly mapped to different values, or additional values added for Accumulo compatibility) intersected with the authorizations of the user that is used
     * to connect to Accumulo. The authorizations returned by the call can be passed to query calls in order to return the maximum amount of data the user is
     * authorized to see. Or, authorizations can be removed from this list to downgrade a query.
     * <p>
     * <strong>WARNING:</strong> If this call is made by a server proxying for a user (and/or other servers) then the response will contain multiple
     * authorizations lists--one per entity. It is up to the caller to decide how to combine them if they wish to present a list to the user for downgrading.
     * Note that {@link AuthorizationsListBase#getAllAuths()}, due to recent changes, actually returns only the user's authorizations. That is, these are the
     * authorizations for the calling or proxied entity that represents a human, otherwise, the calling entity's authorizations. This list is sufficient for use
     * in downgrading, but it should be noted the list is not necessarily representative of data that will be returned from a query. When evaluating data for
     * return to a called, the data is tested against every authorization set listed here, and it must pass all of them to be returned. Therefore, if the user
     * has the authorization FOO, but one of the other proxied entities in the chain does not, no data with FOO will be returned even though
     * listEffectiveAuthorizations indicates the user has that authorization.
     * <p>
     * An example scenario where this comes into play is an external person querying through an internal server. The external party will have an authorization
     * for their organization, say EXT1 for example, whereas the server will have the INT authorization. We want to be able to return data with releasabilities
     * such as {@code INT&EXT1}, {@code INT&ALL_EXT}, but not data such as {@code INT&EXT2}. There is no single merged list of authorizations that will allow
     * the correct data to come back. So, instead we test all data against each authorization set. For the {@code INT&EXT2} data example, the server's
     * authorizations will allow the data to be returned since the server has INT. The user's authorizations will not allow the data to be returned however
     * since the user has neither INT nor EXT2. In this scenario, the result for {@link AuthorizationsListBase#getAllAuths()} will contain both INT and EXT1
     * even though the user does not have INT. The INT auth must be passed when queries are created, else no data would be returned (since the INT auth would be
     * removed from the server's auths and then no data would be returned).
     * <p>
     * For most use cases, a GUI can compute the intersection of all authorizations that are not domains and then include the union of organizations. This will
     * not always be the case, however. Consider data marked with something like: {@code FOO&INT|FOO&EXT}. If a system needs to support querying data such as
     * that, then a simple intersection of everything except organization will no longer work.
     * <p>
     * Note that the the return type can be changed by specifying an Accept header, or by adding a suffix to the request URL. The following suffix to return
     * type mppings are:
     * <ul>
     * <li>txt: text/plain
     * <li>xml: application/xml
     * <li>json: application/json
     * </ul>
     * For example, the URL
     * 
     * <pre>
     * &lt;baseURL&gt;/User/listEffectiveAuthorizations.json
     * </pre>
     * 
     * will return the results in JSON format.
     *
     * @return the user and proxied entities' authorizations
     */
    @GET
    @Path("/listEffectiveAuthorizations")
    @Produces({"application/xml", "text/xml", "text/plain", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "text/html"})
    public AuthorizationsListBase listEffectiveAuthorizations() {
        
        AuthorizationsListBase list = responseObjectFactory.getAuthorizationsList();
        
        Map<String,Set<String>> authMap = new HashMap<>();
        
        // Find out who/what called this method
        Principal p = context.getCallerPrincipal();
        String sid = p.getName();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal datawavePrincipal = (DatawavePrincipal) p;
            sid = datawavePrincipal.getShortName();
            // Add the user DN's auths into the authorization list
            String userDN = datawavePrincipal.getUserDN();
            if (userDN != null) {
                String subjectDN = userDN;
                String issuerDN = null;
                String[] dns = DnUtils.splitProxiedSubjectIssuerDNs(userDN);
                if (dns.length > 1) {
                    subjectDN = dns[0];
                    issuerDN = dns[1];
                } else {
                    List<String> dnList = Arrays.asList(datawavePrincipal.getDNs());
                    int index = dnList.indexOf(userDN);
                    if (index >= 0) {
                        subjectDN = dnList.get(index);
                        if (index < dnList.size() - 1)
                            issuerDN = dnList.get(index + 1);
                    }
                }
                list.setUserAuths(subjectDN, issuerDN, new HashSet<>(datawavePrincipal.getUserAuthorizations()));
            }
            // Now add all entity auth mappings into the list
            for (Entry<String,Collection<String>> entry : datawavePrincipal.getAuthorizationsMap().entrySet()) {
                log.trace(sid + " has " + entry.getKey() + " -> " + entry.getValue());
                String[] dns = DnUtils.splitProxiedSubjectIssuerDNs(entry.getKey());
                String subjectDN = dns[0];
                String issuerDN = entry.getKey();
                if (dns.length == 2)
                    issuerDN = dns[1];
                else if (dns.length == 1)
                    issuerDN = null;
                list.addAuths(subjectDN, issuerDN, new HashSet<>(entry.getValue()));
                
                // Map raw auth service role -> remapped auth service role(s) -> accumulo auth(s)
                Collection<String> rawRoles = datawavePrincipal.getRawRoles(entry.getKey());
                if (rawRoles != null) {
                    for (String role : rawRoles) {
                        String[] remappedAuthServicesRoles = principalFactory.remapRoles(entry.getKey(), new String[] {role});
                        for (int i = 0; i < remappedAuthServicesRoles.length; ++i) {
                            remapRole(remappedAuthServicesRoles[i], remappedAuthServicesRoles[i], authMap);
                            // Put the first role in the mapping with the original role name. This should cover the case of renaming
                            // a role (so long as the auth translator preserves the ordering where the renamed role is added to the
                            // result list before any additional roles.
                            if (i == 0)
                                remapRole(remappedAuthServicesRoles[i], role, authMap);
                        }
                    }
                    
                } else {
                    log.warn("No raw authorization service roles found for " + entry.getKey());
                }
            }
            list.setAuthMapping(authMap);
        }
        log.trace(sid + " has authorizations union " + list.getAllAuths());
        return list;
    }
    
    protected void remapRole(String role, String roleName, Map<String,Set<String>> authMap) {
        Set<String> authsForRole = authMap.get(roleName);
        if (authsForRole == null) {
            authsForRole = new HashSet<>();
            authMap.put(roleName, authsForRole);
        }
        
        String[] auths = principalFactory.toAccumuloAuthorizations(new String[] {role});
        Collections.addAll(authsForRole, auths);
    }
    
    /**
     * Clears any cached credentials for the calling user. The end result is that future calls to other methods on this application will require outside contact
     * with the authentication provider.
     *
     * If the credentials are for a single user with no proxy involved, these are the only credentials flushed. Otherwise, if there is a proxy chain, this will
     * flush the DN for the user in the proxy (assumes there is never more than one user in the proxy chain).
     */
    @GET
    @Path("/flushCachedCredentials")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @PermitAll
    public GenericResponse<String> flushCachedCredentials() {
        GenericResponse<String> response = new GenericResponse<>();
        Principal p = context.getCallerPrincipal();
        log.info("Flushing credentials for " + p + " from the cache.");
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal cp = (DatawavePrincipal) p;
            String[] dns = DnUtils.splitProxiedSubjectIssuerDNs(cp.getUserDN());
            String result = credentialsCacheService.evict(dns[0]);
            response.setResult(result);
        } else {
            log.warn(p + " is not a DatawavePrincipal.  Cannot flush credentials.");
            response.addMessage("Unable to determine calling user name.  Values were not flushed!");
            throw new DatawaveWebApplicationException(new IllegalStateException("Unable to flush credentials.  Unknown principal type."), response);
        }
        
        return response;
    }
    
}
