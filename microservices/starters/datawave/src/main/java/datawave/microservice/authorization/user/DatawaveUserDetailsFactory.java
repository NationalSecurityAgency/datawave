package datawave.microservice.authorization.user;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import datawave.microservice.authorization.config.DatawaveSecurityProperties;
import datawave.security.authorization.DatawaveUser;

/**
 * Constructs DatawaveUserDetails instances with their roles limited by the required roles set specified in our configuration. This will be used to create
 * DatawaveUserDetails instances during REST endpoint requests.
 */
@Component
public class DatawaveUserDetailsFactory {
    private final Set<String> requiredRoles;
    
    @Autowired
    public DatawaveUserDetailsFactory(DatawaveSecurityProperties securityProperties) {
        this.requiredRoles = securityProperties.getRequiredRoles();
    }
    
    public DatawaveUserDetails create(Collection<? extends DatawaveUser> proxiedUsers, long creationTime) {
        List<DatawaveUser> proxiedUserList = new ArrayList<>(proxiedUsers);
        boolean removeRequiredRoles = proxiedUserList.stream().anyMatch(u -> Collections.disjoint(u.getRoles(), requiredRoles));
        List<SimpleGrantedAuthority> roles = DatawaveUserDetails.findPrimaryUser(proxiedUserList).getRoles().stream()
                        .filter(r -> removeRequiredRoles || !requiredRoles.contains(r)).map(SimpleGrantedAuthority::new).collect(Collectors.toList());
        return new DatawaveUserDetails(proxiedUserList, roles, creationTime);
    }
}
