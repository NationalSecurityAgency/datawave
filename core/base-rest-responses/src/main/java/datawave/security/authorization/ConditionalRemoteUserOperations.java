package datawave.security.authorization;

import java.util.Collections;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.user.AuthorizationsListBase;
import datawave.webservice.result.GenericResponse;

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
    private Function<ProxiedUserDetails,Boolean> condition;
    private Supplier<AuthorizationsListBase> authorizationsListBaseSupplier;
    
    private static final GenericResponse<String> EMPTY_RESPONSE = new GenericResponse<>();
    
    public boolean isFiltered(ProxiedUserDetails principal) {
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
    public AuthorizationsListBase listEffectiveAuthorizations(ProxiedUserDetails callerObject) throws AuthorizationException {
        assert (delegate != null);
        assert (condition != null);
        assert (authorizationsListBaseSupplier != null);
        
        if (!isFiltered(callerObject)) {
            return delegate.listEffectiveAuthorizations(callerObject);
        } else {
            AuthorizationsListBase response = authorizationsListBaseSupplier.get();
            response.setUserAuths(callerObject.getPrimaryUser().getDn().subjectDN(), callerObject.getPrimaryUser().getDn().issuerDN(), Collections.EMPTY_LIST);
            return response;
        }
    }
    
    @Override
    public GenericResponse<String> flushCachedCredentials(ProxiedUserDetails callerObject) throws AuthorizationException {
        assert (delegate != null);
        assert (condition != null);
        assert (authorizationsListBaseSupplier != null);
        
        if (!isFiltered(callerObject)) {
            return delegate.flushCachedCredentials(callerObject);
        } else {
            return EMPTY_RESPONSE;
        }
    }
    
    public UserOperations getDelegate() {
        return delegate;
    }
    
    public void setDelegate(UserOperations delegate) {
        this.delegate = delegate;
    }
    
    public Function<ProxiedUserDetails,Boolean> getCondition() {
        return condition;
    }
    
    public void setCondition(Function<ProxiedUserDetails,Boolean> condition) {
        this.condition = condition;
    }
    
    public Supplier<AuthorizationsListBase> getAuthorizationsListBaseSupplier() {
        return authorizationsListBaseSupplier;
    }
    
    public void setAuthorizationsListBaseSupplier(Supplier<AuthorizationsListBase> authorizationsListBaseSupplier) {
        this.authorizationsListBaseSupplier = authorizationsListBaseSupplier;
    }
}
