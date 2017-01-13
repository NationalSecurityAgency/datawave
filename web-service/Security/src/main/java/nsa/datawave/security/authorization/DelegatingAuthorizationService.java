package nsa.datawave.security.authorization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jboss.logging.Logger;

import javax.enterprise.inject.Alternative;

/**
 * A simple {@link AuthorizationService} that delegates to a list of other {@link AuthorizationService}s and uses the fist non-null result returned by any one
 * of the delegates.
 */
@Alternative
public class DelegatingAuthorizationService implements AuthorizationService {
    
    private Logger log = Logger.getLogger(getClass());
    private List<AuthorizationService> delegates = new ArrayList<>();
    
    public void setDelegates(List<AuthorizationService> delegates) {
        this.delegates = delegates;
    }
    
    @Override
    public String[] getRoles(String projectName, String userDN, String issuerDN) {
        String[] roles = null;
        if (log.isTraceEnabled()) {
            log.trace("Retrieving roles for \"" + userDN + "\" / \"" + issuerDN + "\" from delegates.");
        }
        if (delegates != null) {
            if (log.isTraceEnabled()) {
                log.trace("Found configured delegates: " + delegates);
            }
            for (AuthorizationService delegate : delegates) {
                if (log.isTraceEnabled()) {
                    log.trace("Retrieving roles for \"" + userDN + "\" / \"" + issuerDN + "\"  from delegate " + delegate.getClass());
                }
                roles = delegate.getRoles(projectName, userDN, issuerDN);
                if (roles != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("Found roles for \"" + userDN + "\" / \"" + issuerDN + "\"  from delegate " + delegate.getClass() + " = "
                                        + Arrays.asList(roles));
                    }
                    break;
                }
            }
        }
        return roles;
    }
}
