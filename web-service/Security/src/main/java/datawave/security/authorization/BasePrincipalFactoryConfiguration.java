package datawave.security.authorization;

import java.util.Collection;
import java.util.HashSet;

/**
 *
 */
public class BasePrincipalFactoryConfiguration {
    private String stickyUserRole;
    private String stickyServerRole;
    
    private boolean insertDNTypeRoles;
    private String serverRoleName = "ServerDN";
    private String userRoleName = "UserDN";
    private Collection<String> insertOURoles = new HashSet<>();
    
    public String getStickyUserRole() {
        return stickyUserRole;
    }
    
    public void setStickyUserRole(String stickyUserRole) {
        this.stickyUserRole = stickyUserRole;
    }
    
    public String getStickyServerRole() {
        return stickyServerRole;
    }
    
    public void setStickyServerRole(String stickyServerRole) {
        this.stickyServerRole = stickyServerRole;
    }
    
    public boolean isInsertDNTypeRoles() {
        return insertDNTypeRoles;
    }
    
    public void setInsertDNTypeRoles(boolean insertDNTypeRoles) {
        this.insertDNTypeRoles = insertDNTypeRoles;
    }
    
    public String getServerRoleName() {
        return serverRoleName;
    }
    
    public void setServerRoleName(String serverRoleName) {
        this.serverRoleName = serverRoleName;
    }
    
    public String getUserRoleName() {
        return userRoleName;
    }
    
    public void setUserRoleName(String userRoleName) {
        this.userRoleName = userRoleName;
    }
    
    public Collection<String> getInsertOURoles() {
        return insertOURoles;
    }
    
    public void setInsertOURoles(Collection<String> insertOURoles) {
        this.insertOURoles = insertOURoles != null ? insertOURoles : new HashSet<String>();
    }
}
