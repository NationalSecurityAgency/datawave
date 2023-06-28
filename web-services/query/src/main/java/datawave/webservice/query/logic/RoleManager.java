package datawave.webservice.query.logic;

import java.security.Principal;
import java.util.Set;

public interface RoleManager {

    boolean canRunQuery(QueryLogic<?> queryLogic, Principal principal);

    void setRequiredRoles(Set<String> requiredRoles);

    Set<String> getRequiredRoles();

}
