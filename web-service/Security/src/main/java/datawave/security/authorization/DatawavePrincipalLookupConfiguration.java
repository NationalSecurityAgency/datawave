package datawave.security.authorization;

import datawave.configuration.RefreshableScope;
import org.apache.deltaspike.core.api.config.ConfigProperty;
import org.jboss.logging.Logger;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@RefreshableScope
public class DatawavePrincipalLookupConfiguration implements Serializable {
    private static final long serialVersionUID = 3382254663037795738L;
    
    protected Logger log = Logger.getLogger(getClass());
    
    @Inject
    @ConfigProperty(name = "dw.principalLookup.projectName")
    protected String projectName;
    @Inject
    @ConfigProperty(name = "dw.principalLookup.requiredRoles")
    protected List<String> requiredRoles;
    
    public String getProjectName() {
        return projectName;
    }
    
    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
    
    public List<String> getRequiredRoles() {
        if (requiredRoles == null) {
            return Collections.emptyList();
        } else {
            return Collections.unmodifiableList(requiredRoles);
        }
    }
}
