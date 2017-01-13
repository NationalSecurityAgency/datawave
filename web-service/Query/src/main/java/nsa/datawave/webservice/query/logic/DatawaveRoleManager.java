package nsa.datawave.webservice.query.logic;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import nsa.datawave.security.authorization.DatawavePrincipal;

import com.google.common.collect.Sets;

public class DatawaveRoleManager implements RoleManager {
    
    private Set<String> requiredRoles;
    
    public DatawaveRoleManager() {}
    
    public DatawaveRoleManager(Collection<String> requiredRoles) {
        this.requiredRoles = Collections.unmodifiableSet(Sets.newHashSet(requiredRoles));
    }
    
    @Override
    public boolean canRunQuery(QueryLogic<?> queryLogic, Principal principal) {
        if (principal instanceof DatawavePrincipal == false)
            return false;
        DatawavePrincipal datawavePrincipal = (DatawavePrincipal) principal;
        if (requiredRoles != null && requiredRoles.size() > 0) {
            Set<String> usersRoles = new HashSet<>();
            Map<String,Collection<String>> userRolesMap = datawavePrincipal.getUserRolesMap();
            if (userRolesMap.size() == 1) {
                usersRoles.addAll(userRolesMap.values().iterator().next());
            } else if (userRolesMap.size() > 1) {
                String userDN = datawavePrincipal.getUserDN();
                for (Entry<String,Collection<String>> entry : userRolesMap.entrySet()) {
                    if (entry.getKey().contains(userDN)) {
                        usersRoles.addAll(entry.getValue());
                        break;
                    }
                }
            }
            if (usersRoles.containsAll(requiredRoles) == false) {
                return false;
            }
        }
        return true;
    }
    
    public Set<String> getRequiredRoles() {
        return requiredRoles;
    }
    
    public void setRequiredRoles(Set<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }
    
}
