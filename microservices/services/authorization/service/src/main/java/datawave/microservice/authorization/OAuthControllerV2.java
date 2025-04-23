package datawave.microservice.authorization;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.oauth.OAuthTokenResponse;
import datawave.security.authorization.oauth.OAuthUserInfo;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * Presents the REST operations for the authorization service to implement the OAuth2 code flow.
 */
@Tag(name = "OAuth Operations /v2",
                externalDocs = @ExternalDocumentation(description = "Authorization Service Documentation",
                                url = "https://github.com/NationalSecurityAgency/datawave-authorization-service"))
@RestController
@RequestMapping(path = "/v2/oauth", produces = MediaType.APPLICATION_JSON_VALUE)
public class OAuthControllerV2 {
    
    private final OAuthOperationsV2 oauthOperations;
    
    @Autowired
    public OAuthControllerV2(OAuthOperationsV2 oauthOperations) {
        this.oauthOperations = oauthOperations;
    }
    
    @Operation(summary = "Authorizes the calling user to produce a JWT value",
                    description = "The returned JWT can be passed to other calls in a header. For example: \"Authorization: bearer <JWT value>\".<br>"
                                    + "The user can be determined with from the supplied client certificate or trusted headers ("
                                    + "X-SSL-clientcert-subject/X-SSL-clientcert-issuer).")
    @RequestMapping(path = "/authorize", method = RequestMethod.GET)
    public void authorize(@AuthenticationPrincipal DatawaveUserDetails currentUser, HttpServletResponse response, @RequestParam String client_id,
                    @RequestParam String redirect_uri, @RequestParam String response_type, @RequestParam(required = false) String state)
                    throws IllegalArgumentException, IOException {
        oauthOperations.authorize(currentUser, response, client_id, redirect_uri, response_type, state);
    }
    
    @Operation(summary = "Authorizes the calling user to produce a JWT value",
                    description = "The returned JWT can be passed to other calls in a header. For example: \"Authorization: bearer <JWT value>\".<br>"
                                    + "The user can be determined with from the supplied client certificate or trusted headers ("
                                    + "X-SSL-clientcert-subject/X-SSL-clientcert-issuer).")
    @RequestMapping(path = "/token", method = RequestMethod.POST)
    public OAuthTokenResponse token(@AuthenticationPrincipal DatawaveUserDetails currentUser, HttpServletResponse response, @RequestParam String grant_type,
                    @RequestParam String client_id, @RequestParam String client_secret, @RequestParam(required = false) String code,
                    @RequestParam(required = false) String refresh_token, @RequestParam(required = false) String redirect_uri) throws IOException {
        return oauthOperations.token(currentUser, response, grant_type, client_id, client_secret, code, refresh_token, redirect_uri);
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    @Operation(summary = "Returns details about the current primary user.",
                    description = "The user can be determined from the supplied client certificate, trusted headers ("
                                    + "X-SSL-clientcert-subject/X-SSL-clientcert-issuer), or Authorization Bearer JWT."
                                    + "Proxied user headers (X-ProxiedEntitiesChain/X-ProxiedIssuersChain) "
                                    + "are also used to determine proxied users to include in the returned details.")
    @RequestMapping(path = "/user", method = RequestMethod.GET)
    public OAuthUserInfo user(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return oauthOperations.user(currentUser);
    }
    
    /**
     * Returns the {@link DatawaveUserDetails} that represents the authenticated calling user.
     */
    @Operation(summary = "Returns details about the current user/proxied users.",
                    description = "The user can be determined from the supplied client certificate, trusted headers ("
                                    + "X-SSL-clientcert-subject/X-SSL-clientcert-issuer), or Authorization Bearer JWT."
                                    + "Proxied user headers (X-ProxiedEntitiesChain/X-ProxiedIssuersChain) "
                                    + "are also used to determine proxied users to include in the returned details.")
    @RequestMapping(path = "/users", method = RequestMethod.GET)
    public Collection<OAuthUserInfo> users(@AuthenticationPrincipal DatawaveUserDetails currentUser) {
        return oauthOperations.users(currentUser);
    }
}
