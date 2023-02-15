package datawave.security.authorization.remote;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import datawave.user.AuthorizationsListBase;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A conditional remote user operations will only invoke the delegate remote service base on a specified function of the local principal
 */
public class ConditionalRemoteUserOperations implements UserOperations {
    private static final Logger log = LoggerFactory.getLogger(ConditionalRemoteUserOperations.class);
    
    private UserOperations delegate;
    private Function<DatawavePrincipal,Boolean> condition;
    private ResponseObjectFactory responseObjectFactory;
    
    private static final GenericResponse<String> EMPTY_RESPONSE = new GenericResponse<>();
    
    @Override
    public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
        if (condition.apply((DatawavePrincipal) callerObject)) {
            return delegate.listEffectiveAuthorizations(callerObject);
        } else {
            return getAuthorizationsList(callerObject);
        }
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(Object callerObject) throws AuthorizationException {
        if (condition.apply((DatawavePrincipal) callerObject)) {
            return delegate.flushCachedCredentials(callerObject);
        } else {
            return EMPTY_RESPONSE;
        }
    }
    
    private AuthorizationsListBase getAuthorizationsList(Object callerObject) {
        AuthorizationsListBase auths = responseObjectFactory.getAuthorizationsList();
        if (callerObject instanceof DatawavePrincipal) {
            DatawavePrincipal principal = (DatawavePrincipal) callerObject;
            Map<AuthorizationsListBase.SubjectIssuerDNPair,Set<String>> authMap = new HashMap<>();
            principal.getProxiedUsers().forEach(u -> authMap.put(dn(u.getDn()), new HashSet<>(u.getAuths())));
            DatawaveUser primaryUser = principal.getPrimaryUser();
            AuthorizationsListBase.SubjectIssuerDNPair primaryDn = dn(primaryUser.getDn());
            auths.setUserAuths(primaryDn.subjectDN, primaryDn.issuerDN, authMap.get(dn(primaryUser.getDn())));
            authMap.entrySet().stream().filter(e -> !e.getKey().equals(primaryDn))
                            .forEach(e -> auths.addAuths(e.getKey().subjectDN, e.getKey().issuerDN, e.getValue()));
        }
        return auths;
    }
    
    public static AuthorizationsListBase.SubjectIssuerDNPair dn(SubjectIssuerDNPair dn) {
        return new AuthorizationsListBase.SubjectIssuerDNPair(dn.subjectDN(), dn.issuerDN());
    }
    
    public UserOperations getDelegate() {
        return delegate;
    }
    
    public void setDelegate(UserOperations delegate) {
        this.delegate = delegate;
    }
    
    public Function<DatawavePrincipal,Boolean> getCondition() {
        return condition;
    }
    
    public void setCondition(Function<DatawavePrincipal,Boolean> condition) {
        this.condition = condition;
    }
    
    public ResponseObjectFactory getResponseObjectFactory() {
        return responseObjectFactory;
    }
    
    public void setResponseObjectFactory(ResponseObjectFactory responseObjectFactory) {
        this.responseObjectFactory = responseObjectFactory;
    }
}
