package datawave.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.SpringBean;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.ProxiedUserDetails;
import datawave.security.authorization.UserOperations;
import datawave.security.cache.CredentialsCacheBean;
import datawave.security.util.WSAuthorizationsUtil;
import datawave.user.AuthorizationsListBase;
import datawave.webservice.common.exception.DatawaveWebApplicationException;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;

@Path("/Security/User")
@LocalBean
@Stateless
@RunAs("InternalUser")
@RolesAllowed({"InternalUser", "AuthorizedUser", "AuthorizedServer", "AuthorizedQueryServer", "SecurityUser"})
@DeclareRoles({"InternalUser", "AuthorizedUser", "AuthorizedServer", "AuthorizedQueryServer", "SecurityUser"})
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class UserOperationsBean implements UserOperations {
    private Logger log = LoggerFactory.getLogger(getClass());

    @Resource
    private EJBContext context;

    @Inject
    private CredentialsCacheBean credentialsCache;

    @Inject
    private ResponseObjectFactory responseObjectFactory;

    @Inject
    @SpringBean(name = "RemoteUserOperationsList")
    private List<UserOperations> remoteUserOperationsList;

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
     * @param includeRemoteServices
     *            An optional query parameter to include any remote service operations in the response. Defaults to true.
     * @return the user and proxied entities' authorizations
     */
    @GET
    @Path("/listEffectiveAuthorizations")
    @Produces({"application/xml", "text/xml", "text/plain", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "text/html"})
    public AuthorizationsListBase listEffectiveAuthorizations(@DefaultValue("true") @QueryParam("includeRemoteServices") boolean includeRemoteServices) {
        return listEffectiveAuthorizations(context.getCallerPrincipal(), includeRemoteServices);
    }

    @Override
    public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails p) {
        return listEffectiveAuthorizations(p, true);
    }

    private AuthorizationsListBase listEffectiveAuthorizations(Object p, boolean includeRemoteServices) {
        final AuthorizationsListBase list = responseObjectFactory.getAuthorizationsList();

        String name = p.toString();
        if (p instanceof DatawavePrincipal) {
            DatawavePrincipal datawavePrincipal = (DatawavePrincipal) p;
            name = datawavePrincipal.getShortName();

            // if we have any remote services configured, merge those authorizations in here
            if (includeRemoteServices && CollectionUtils.isNotEmpty(remoteUserOperationsList)) {
                Set<String> localDNs = new HashSet<>(Arrays.asList(datawavePrincipal.getDNs()));
                log.debug("Verifying remote principals cover {}", localDNs);
                List<DatawaveUser> reducedRemoteProxiedUsers = new ArrayList<>();

                for (UserOperations remote : remoteUserOperationsList) {
                    try {
                        DatawavePrincipal remotePrincipal = remote.getRemoteUser(datawavePrincipal);

                        Set<String> remoteDNs = new HashSet<>(Arrays.asList(remotePrincipal.getDNs()));
                        log.debug("Checking remote principal list {}", remoteDNs);
                        if (!remoteDNs.containsAll(localDNs)) {
                            log.warn("Skipping remote user: " + localDNs + " was not contained by " + remoteDNs);
                            continue;
                        }

                        for (DatawaveUser user : remotePrincipal.getProxiedUsers()) {
                            if (localDNs.contains(user.getDn().subjectDN())) {
                                reducedRemoteProxiedUsers.add(user);
                            } else {
                                log.debug("{} was a remote only user and has been removed", user.getDn().subjectDN());
                                log.trace("Remote only user DN and Authorizations: {} : {}", user.getDn().subjectDN(), user.getAuths());
                            }
                        }

                    } catch (Exception e) {
                        log.error("Failed to lookup users from remote user service", e);
                        list.addMessage("Failed to include user from remote service: " + e.getMessage());
                    }
                }

                reducedRemoteProxiedUsers.add(datawavePrincipal.getPrimaryUser());
                DatawavePrincipal reducedRemotePrincipal = new DatawavePrincipal(reducedRemoteProxiedUsers);
                datawavePrincipal = WSAuthorizationsUtil.mergePrincipals(datawavePrincipal, reducedRemotePrincipal);
            }

            // Add the user DN's auths into the authorization list
            DatawaveUser primaryUser = datawavePrincipal.getPrimaryUser();

            list.setUserAuths(primaryUser.getDn().subjectDN(), primaryUser.getDn().issuerDN(), new HashSet<>(primaryUser.getAuths()));
            // Now add all entity auth sets into the list
            datawavePrincipal.getProxiedUsers().forEach(u -> list.addAuths(u.getDn().subjectDN(), u.getDn().issuerDN(), new HashSet<>(u.getAuths())));

            // Add the role to authorization mapping.
            // NOTE: Currently this is only added for the primary user, which is really all anyone should care about in terms of mucking with
            // authorizations. When used for queries, all non-primary users have all of their auths included -- there is no downgrading.
            list.setAuthMapping(datawavePrincipal.getPrimaryUser().getRoleToAuthMapping().asMap());
        }

        log.trace(name + " has authorizations union " + list.getAllAuths());
        return list;
    }

    /**
     * Clears any cached credentials for the calling user. The end result is that future calls to other methods on this application will require outside contact
     * with the authentication provider.
     *
     * If the credentials are for a single user with no proxy involved, these are the only credentials flushed. Otherwise, if there is a proxy chain, this will
     * flush the DN for the user in the proxy (assumes there is never more than one user in the proxy chain).
     *
     * @param includeRemoteServices
     *            An optional query parameter to include any remote service operations in the response. Defaults to true.
     * @return A generic response denoting success or failure
     */
    @GET
    @Path("/flushCachedCredentials")
    @Produces({"application/xml", "text/xml", "application/json", "text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf",
            "application/x-protostuff"})
    @PermitAll
    public GenericResponse<String> flushCachedCredentials(@DefaultValue("true") @QueryParam("includeRemoteServices") boolean includeRemoteServices) {
        return flushCachedCredentials((ProxiedUserDetails) context.getCallerPrincipal(), includeRemoteServices);
    }

    @Override
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerPrincipal) {
        return flushCachedCredentials(callerPrincipal, true);
    }

    private GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerPrincipal, boolean includeRemoteServices) {
        GenericResponse<String> response = new GenericResponse<>();
        log.info("Flushing credentials for " + callerPrincipal + " from the cache.");

        // if we have any remote services configured, then flush those credentials as well
        if (includeRemoteServices && CollectionUtils.isNotEmpty(remoteUserOperationsList)) {
            for (UserOperations remote : remoteUserOperationsList) {
                try {
                    remote.flushCachedCredentials(callerPrincipal);
                } catch (Exception e) {
                    log.error("Failed to flush user from remote user service", e);
                    response.addMessage("Unable to user from remote user service");
                    response.addException(e);
                }
            }
        }

        if (callerPrincipal instanceof DatawavePrincipal) {
            DatawavePrincipal dp = (DatawavePrincipal) callerPrincipal;
            response.setResult(credentialsCache.evict(dp.getUserDN().subjectDN()));
        } else {
            log.warn(callerPrincipal + " is not a DatawavePrincipal.  Cannot flush credentials.");
            response.addMessage("Unable to determine calling user name.  Values were not flushed!");
            throw new DatawaveWebApplicationException(new IllegalStateException("Unable to flush credentials.  Unknown principal type."), response);
        }

        return response;
    }

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

}
