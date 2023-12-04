package datawave.security.authorization.remote;

import datawave.security.authorization.AuthorizationException;
import datawave.security.authorization.DatawavePrincipal;
import datawave.user.AuthorizationsListBase;
import datawave.security.authorization.UserOperations;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.GenericResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.function.Function;

/**
 * A conditional remote user operations will only invoke the delegate remote service base on a specified function of the specified principal. For example we may
 * only need to invoke the remote user operations if we know the remote system will have additional auths that this user will need for the query logic being
 * invoked.
 *
 * An example may be a composite query that call a local and a remote query logic. Perhaps we can already tell that the user will not be able to get any
 * additional authorities from the remote system and hence the remote call will not be required.
 */
public class ConditionalRemoteUserOperations implements UserOperations {
    private static final Logger log = LoggerFactory.getLogger(ConditionalRemoteUserOperations.class);
    
    private UserOperations delegate;
    private Function<DatawavePrincipal,Boolean> condition;
    private ResponseObjectFactory responseObjectFactory;
    
    private static final GenericResponse<String> EMPTY_RESPONSE = new GenericResponse<>();
    
    public boolean isFiltered(DatawavePrincipal principal) {
        if (!condition.apply(principal)) {
            if (log.isDebugEnabled()) {
                log.debug("Filter " + condition + " blocking " + principal.getName() + " from " + delegate + " user operations");
            }
            return true;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Passing through filter " + condition + " for " + principal.getName() + " for " + delegate + " user operations");
            }
            return false;
        }
    }
    
    @Override
    public AuthorizationsListBase listEffectiveAuthorizations(Object callerObject) throws AuthorizationException {
        assert (delegate != null);
        assert (condition != null);
        assert (responseObjectFactory != null);
        
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        
        if (!isFiltered(principal)) {
            return delegate.listEffectiveAuthorizations(callerObject);
        } else {
            AuthorizationsListBase response = responseObjectFactory.getAuthorizationsList();
            response.setUserAuths(principal.getUserDN().subjectDN(), principal.getUserDN().issuerDN(), Collections.EMPTY_LIST);
            return response;
        }
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(Object callerObject) throws AuthorizationException {
        assert (delegate != null);
        assert (condition != null);
        assert (responseObjectFactory != null);
        
        final DatawavePrincipal principal = getDatawavePrincipal(callerObject);
        
        if (!isFiltered(principal)) {
            return delegate.flushCachedCredentials(callerObject);
        } else {
            return EMPTY_RESPONSE;
        }
    }
    
    private DatawavePrincipal getDatawavePrincipal(Object callerObject) {
        if (callerObject instanceof DatawavePrincipal) {
            return (DatawavePrincipal) callerObject;
        }
        throw new RuntimeException("Cannot handle a " + callerObject.getClass() + ". Only DatawavePrincipal is accepted");
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
