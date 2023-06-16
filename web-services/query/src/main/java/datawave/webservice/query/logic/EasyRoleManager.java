package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

public class EasyRoleManager implements RoleManager {

    @Override
    public boolean canRunQuery(QueryLogic<?> queryLogic, Principal principal) {
        return true;
    }

    @Override
    public void setRequiredRoles(Set<String> requiredRoles) {
        // TODO Auto-generated method stub
    }

    @Override
    public Set<String> getRequiredRoles() {
        return Collections.emptySet();
    }
}
