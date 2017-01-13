package nsa.datawave.security.authorization.simple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import nsa.datawave.security.authorization.AuthorizationService;

import javax.enterprise.inject.Alternative;

@Alternative
public class SimpleAuthorizationService implements AuthorizationService {
    
    private List<String> userRoles = new ArrayList<>();
    
    public SimpleAuthorizationService() {
        
    }
    
    @Override
    public String[] getRoles(String projectName, String userDN, String issuerDN) {
        
        String[] userRolesArray = new String[userRoles.size()];
        return Collections.unmodifiableList(userRoles).toArray(userRolesArray);
    }
    
    public List<String> getUserRoles() {
        return userRoles;
    }
    
    public void setUserRoles(List<String> userRoles) {
        this.userRoles = userRoles;
    }
}
