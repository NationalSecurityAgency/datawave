package datawave.microservice.authorization;

import static datawave.security.authorization.oauth.OAuthConstants.GRANT_AUTHORIZATION_CODE;
import static datawave.security.authorization.oauth.OAuthConstants.GRANT_REFRESH_TOKEN;
import static datawave.security.authorization.oauth.OAuthConstants.RESPONSE_TYPE_CODE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import datawave.microservice.authorization.oauth.AuthorizationRequest;
import datawave.microservice.authorization.oauth.AuthorizedClient;
import datawave.microservice.authorization.oauth.OAuthProperties;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.CachedDatawaveUserService;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.JWTTokenHandler;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.security.authorization.oauth.OAuthTokenResponse;
import datawave.security.authorization.oauth.OAuthUserInfo;
import io.jsonwebtoken.ExpiredJwtException;

/**
 * Presents the REST operations for the authorization service to implement the OAuth2 code flow.
 */
@Service
public class OAuthOperationsV2 {
    private Logger log = LoggerFactory.getLogger(getClass());
    private final JWTTokenHandler tokenHandler;
    private final CachedDatawaveUserService cachedDatawaveUserService;
    private final OAuthProperties oAuthProperties;
    
    private Cache authCache;
    private Map<String,Long> authExpirationMap = new ConcurrentHashMap<>();
    
    @Autowired
    public OAuthOperationsV2(JWTTokenHandler tokenHandler, CachedDatawaveUserService cachedDatawaveUserService, OAuthProperties oAuthProperties,
                    CacheManager cacheManager) {
        this.tokenHandler = tokenHandler;
        this.cachedDatawaveUserService = cachedDatawaveUserService;
        this.oAuthProperties = oAuthProperties;
        this.authCache = cacheManager.getCache("oauthAuthorizations");
    }
    
    public void authorize(@AuthenticationPrincipal DatawaveUserDetails currentUser, HttpServletResponse response, @RequestParam String client_id,
                    @RequestParam String redirect_uri, @RequestParam String response_type, @RequestParam(required = false) String state)
                    throws IllegalArgumentException, IOException {
        
        AuthorizedClient client = this.oAuthProperties.getAuthorizedClients().get(client_id);
        if (client == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "unauthorized_client (client_id not registered)");
            return;
        }
        if (!response_type.equalsIgnoreCase(RESPONSE_TYPE_CODE)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (response_type must be '" + RESPONSE_TYPE_CODE + "')");
            return;
        }
        String code = RandomStringUtils.random(40, true, true);
        putAuthorizationRequest(code, new AuthorizationRequest(currentUser, client, redirect_uri));
        StringBuilder builder = new StringBuilder();
        builder.append(redirect_uri);
        builder.append("?");
        builder.append("code=").append(code);
        if (StringUtils.isNotBlank(state)) {
            builder.append("&state=").append(state);
        }
        response.sendRedirect(builder.toString());
    }
    
    public OAuthTokenResponse token(@AuthenticationPrincipal DatawaveUserDetails currentUser, HttpServletResponse response, @RequestParam String grant_type,
                    @RequestParam String client_id, @RequestParam String client_secret, @RequestParam(required = false) String code,
                    @RequestParam(required = false) String refresh_token, @RequestParam(required = false) String redirect_uri) throws IOException {
        
        AuthorizedClient client = this.oAuthProperties.getAuthorizedClients().get(client_id);
        if (client == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "unauthorized_client (client_id not registered)");
            return null;
        }
        if (!client_secret.equals(client.getClient_secret())) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "unauthorized_client - (incorrect client_secret)");
            return null;
        }
        
        Collection<SubjectIssuerDNPair> userDnsToLookupAndAdd = new LinkedHashSet<>();
        if (grant_type.equals(GRANT_AUTHORIZATION_CODE)) {
            if (StringUtils.isBlank(code)) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (must supply code for grant_type authorization_code)");
                return null;
            }
            AuthorizationRequest authorizationRequest = getAuthorizationRequest(code);
            if (authorizationRequest == null) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (requested code not found)");
                return null;
            }
            AuthorizedClient authorizedClient = authorizationRequest.getAuthorizedClient();
            if (!authorizedClient.getClient_id().equals(client_id) || !authorizedClient.getClient_secret().equals(client_secret)) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (client_id/client_secret do not match authorize request)");
                return null;
            }
            // a code can only be used once
            removeAuthorizationRequest(code);
            if (!redirect_uri.equals(authorizationRequest.getRedirect_uri())) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (redirect_uri must match the value when authorize was called)");
                return null;
            }
            log.debug("Issuing a token for " + authorizationRequest.getDatawaveUserDetails().getPrimaryUser().getCommonName() + " to "
                            + client.getClient_name());
            authorizationRequest.getDatawaveUserDetails().getProxiedUsers().forEach(u -> userDnsToLookupAndAdd.add(u.getDn()));
            // Add dn for the DN corresponding to the client that is invoking this call
            // Required for authorization_code path, but not refresh_token path since all DNs will be in refresh token
            currentUser.getProxiedUsers().forEach(u -> userDnsToLookupAndAdd.add(u.getDn()));
        } else if (grant_type.equals(GRANT_REFRESH_TOKEN)) {
            if (StringUtils.isBlank(refresh_token)) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_request (must provide refresh_token for grant_type refresh_token)");
                return null;
            }
            // Get the userDns from the principals that were encoded in the refresh_token
            List<DatawaveUser> usersInRefreshToken = new ArrayList<>();
            try {
                usersInRefreshToken.addAll(tokenHandler.createUsersFromToken(refresh_token, JWTTokenHandler.REFRESH_TOKEN_CLAIM));
                for (DatawaveUser user : usersInRefreshToken) {
                    userDnsToLookupAndAdd.add(user.getDn());
                }
            } catch (ExpiredJwtException e) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, "invalid_grant (refresh_token expired)");
                return null;
            }
            log.debug("Refreshing token for " + usersInRefreshToken.get(0).getCommonName() + " to " + client.getClient_name());
        } else {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                            "invalid_grant (grant_type must be 'authorization_code' or 'refresh_token' " + grant_type + " not supported)");
            return null;
        }
        Collection<DatawaveUser> proxiedUsers = new LinkedHashSet<>();
        try {
            // bypass the cache and lookup DatawaveUsers
            proxiedUsers.addAll(cachedDatawaveUserService.lookup(userDnsToLookupAndAdd));
        } catch (AuthorizationException e) {
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
            return null;
        }
        String name = proxiedUsers.stream().map(DatawaveUser::getName).collect(Collectors.joining(" -> "));
        long now = System.currentTimeMillis();
        Date idTokenExpire = new Date(now + this.oAuthProperties.getIdTokenTtl(TimeUnit.MILLISECONDS));
        String idToken = tokenHandler.createTokenFromUsers(name, proxiedUsers, JWTTokenHandler.PRINCIPALS_CLAIM, idTokenExpire);
        // Create DatawaveUsers with no auths, roles, or mapping for the refresh token
        // The OAuth service can identify the user with this token, but it will have no auths/roles
        // It is also serialized under a different claim ("refresh") than the access_token ("principals")
        Date refreshTokenExpire = new Date(now + this.oAuthProperties.getRefreshTokenTtl(TimeUnit.MILLISECONDS));
        Set<DatawaveUser> usersForRefreshToken = new LinkedHashSet<>();
        for (DatawaveUser u : proxiedUsers) {
            usersForRefreshToken.add(new DatawaveUser(u.getDn(), u.getUserType(), u.getEmail(), null, null, null, now));
        }
        String refreshToken = tokenHandler.createTokenFromUsers(name, usersForRefreshToken, JWTTokenHandler.REFRESH_TOKEN_CLAIM, refreshTokenExpire);
        return new OAuthTokenResponse(idToken, idToken, refreshToken, this.oAuthProperties.getIdTokenTtl(TimeUnit.SECONDS));
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    public OAuthUserInfo user(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return new OAuthUserInfo(currentUser.getPrimaryUser());
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    public Collection<OAuthUserInfo> users(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        List<OAuthUserInfo> users = new ArrayList<>();
        currentUser.getProxiedUsers().forEach(u -> users.add(new OAuthUserInfo(u)));
        return users;
    }
    
    private AuthorizationRequest getAuthorizationRequest(String code) {
        return this.authCache.get(code, AuthorizationRequest.class);
    }
    
    private void putAuthorizationRequest(String code, AuthorizationRequest authRequest) {
        this.authCache.put(code, authRequest);
        this.authExpirationMap.put(code, (System.currentTimeMillis() + this.oAuthProperties.getAuthCodeTtl(TimeUnit.MILLISECONDS)));
    }
    
    private void removeAuthorizationRequest(String code) {
        this.authCache.evict(code);
        this.authExpirationMap.remove(code);
    }
    
    @Scheduled(fixedDelay = 1000)
    private void expireAuthRequests() {
        if (!authExpirationMap.isEmpty()) {
            long now = System.currentTimeMillis();
            synchronized (authExpirationMap) {
                this.authExpirationMap.entrySet().forEach(e -> {
                    if (now > e.getValue()) {
                        this.authCache.evict(e.getKey());
                    }
                });
            }
        }
    }
}
