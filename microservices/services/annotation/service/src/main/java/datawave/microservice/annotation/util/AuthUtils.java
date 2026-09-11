package datawave.microservice.annotation.util;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHeaders;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

import datawave.microservice.annotation.util.exceptions.AuthenticationException;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuthUtils {
    private static final String PROXIED_ENTITIES_HEADER = "X-ProxiedEntitiesChain";
    private static final String PROXIED_ISSUERS_HEADER = "X-ProxiedIssuersChain";

    public static PreAuthenticatedAuthenticationToken getAuthToken(Principal currentUser) {
        if (currentUser instanceof PreAuthenticatedAuthenticationToken) {
            return (PreAuthenticatedAuthenticationToken) currentUser;
        }
        throw new AuthenticationException("Cannot handle a " + currentUser.getClass() + ". Only PreAuthenticatedAuthenticationToken is accepted");
    }

    public static Map<String,String> buildOutgoingHeaders(DatawaveUserDetails proxiedUserDetails, String userAgent) {
        HashMap<String,String> headers = new HashMap<>();

        // Pass original user agent
        if (userAgent != null) {
            headers.put(HttpHeaders.USER_AGENT, userAgent);
        }

        // Build proxied entities headers
        if (proxiedUserDetails == null) {
            return headers;
        }
        StringBuilder userDnChain = new StringBuilder();
        StringBuilder issuerDnChain = new StringBuilder();
        for (DatawaveUser user : proxiedUserDetails.getProxiedUsers()) {
            SubjectIssuerDNPair dnPair = user.getDn();
            userDnChain.append('<').append(dnPair.subjectDN()).append('>');
            issuerDnChain.append('<').append(dnPair.issuerDN()).append('>');
        }

        headers.put(PROXIED_ENTITIES_HEADER, userDnChain.toString());
        headers.put(PROXIED_ISSUERS_HEADER, issuerDnChain.toString());
        log.debug("Setting proxied entity headers: {}", headers);
        return headers;
    }
}
