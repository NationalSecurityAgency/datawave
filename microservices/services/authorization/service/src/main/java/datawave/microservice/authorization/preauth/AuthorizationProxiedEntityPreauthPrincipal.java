package datawave.microservice.authorization.preauth;

import java.util.Collection;

import javax.servlet.http.HttpServletRequest;

import datawave.security.authorization.SubjectIssuerDNPair;

public class AuthorizationProxiedEntityPreauthPrincipal extends ProxiedEntityPreauthPrincipal {
    
    private HttpServletRequest request;
    
    public AuthorizationProxiedEntityPreauthPrincipal(SubjectIssuerDNPair callerPrincipal, Collection<SubjectIssuerDNPair> proxiedEntities,
                    HttpServletRequest request) {
        super(callerPrincipal, proxiedEntities);
        this.request = request;
    }
    
    public HttpServletRequest getRequest() {
        return request;
    }
}
