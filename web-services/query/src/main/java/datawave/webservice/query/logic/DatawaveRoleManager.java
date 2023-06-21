package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import datawave.security.authorization.DatawavePrincipal;

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
        if (requiredRoles != null && !requiredRoles.isEmpty()) {
            Set<String> usersRoles = new HashSet<>(datawavePrincipal.getPrimaryUser().getRoles());
            return usersRoles.containsAll(requiredRoles);
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
