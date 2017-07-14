package datawave.security.authorization;

import java.util.List;
import java.util.Map;

import javax.enterprise.inject.Alternative;
import javax.inject.Inject;

import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.configuration.spring.SpringBean;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.jboss.logging.Logger;

/**
 * A {@link AuthorizationService} that simply returns responses from an in-memory map that is supplied via a Spring bean named
 * {@code testAuthorizationServiceRoles}.
 */
@Alternative
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class TestAuthorizationService implements AuthorizationService {
    private Logger log = Logger.getLogger(getClass());
    
    @Inject
    @SpringBean(name = "testAuthorizationServiceRoles", refreshable = true)
    private Map dnToRoleMap;
    
    @Override
    public String[] getRoles(String projectName, String userDN, String issuerDN) {
        String[] roles = null;
        if (log.isTraceEnabled()) {
            log.trace("Retrieving roles for \"" + userDN + "\" from configured map.");
        }
        if (dnToRoleMap != null) {
            if (log.isTraceEnabled()) {
                log.trace("Found configured map: " + dnToRoleMap);
            }
            List<String> dnRoles = (List<String>) dnToRoleMap.get(userDN);
            if (dnRoles != null) {
                if (log.isTraceEnabled()) {
                    log.trace("Found roles for \"" + userDN + "\" from configured map: " + dnRoles);
                }
                roles = dnRoles.toArray(new String[dnRoles.size()]);
            }
        }
        return roles;
    }
}
