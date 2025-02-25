package datawave.microservice.authorization.preauth;

import javax.servlet.http.HttpServletRequest;

import org.springframework.security.web.AuthenticationEntryPoint;

public class AuthorizationProxiedEntityX509Filter extends ProxiedEntityX509Filter {
    
    public AuthorizationProxiedEntityX509Filter(boolean useTrustedSubjectHeaders, boolean requireProxiedEntities, boolean requireIssuers,
                    AuthenticationEntryPoint authenticationEntryPoint) {
        super(useTrustedSubjectHeaders, requireProxiedEntities, requireIssuers, authenticationEntryPoint);
    }
    
    @Override
    protected Object getPreAuthenticatedPrincipal(HttpServletRequest request) {
        ProxiedEntityPreauthPrincipal principal = (ProxiedEntityPreauthPrincipal) super.getPreAuthenticatedPrincipal(request);
        if (principal == null) {
            return principal;
        } else {
            return new AuthorizationProxiedEntityPreauthPrincipal(principal.getCallerPrincipal(), principal.getProxiedEntities(), request);
        }
    }
}
