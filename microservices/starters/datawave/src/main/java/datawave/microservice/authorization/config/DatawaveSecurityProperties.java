package datawave.microservice.authorization.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * Datawave-specific extensions to {@link SecurityProperties}
 */
@ConfigurationProperties(prefix = "spring.security.datawave")
public class DatawaveSecurityProperties {
    @NestedConfigurationProperty
    private final Jwt jwt = new Jwt();
    private boolean useTrustedSubjectHeaders;
    private boolean proxiedEntitiesRequired;
    private boolean issuersRequired;
    private boolean enforceAllowedCallers = true;
    private List<String> allowedCallers = new ArrayList<>();
    private boolean requireSsl;
    private List<String> managerRoles = new ArrayList<>();
    
    /**
     * The denied access role is a single role that must not be present in the proxy chain. If the role is present, then access will be denied for the user.
     */
    private String deniedAccessRole = null;
    
    /**
     * Required roles are a set of roles such that each entity in a proxy chain must have at least one of the required roles. If that is not the case, then the
     * user role filter will ensure that none of the required roles are included in the response to getAuthorities.
     */
    private Set<String> requiredRoles = new HashSet<>(Arrays.asList("AuthorizedUser", "AuthorizedServer", "AuthorizedQueryServer"));
    
    public boolean isUseTrustedSubjectHeaders() {
        return useTrustedSubjectHeaders;
    }
    
    public void setUseTrustedSubjectHeaders(boolean useTrustedSubjectHeaders) {
        this.useTrustedSubjectHeaders = useTrustedSubjectHeaders;
    }
    
    public boolean isProxiedEntitiesRequired() {
        return proxiedEntitiesRequired;
    }
    
    public void setProxiedEntitiesRequired(boolean proxiedEntitiesRequired) {
        this.proxiedEntitiesRequired = proxiedEntitiesRequired;
    }
    
    public boolean isIssuersRequired() {
        return issuersRequired;
    }
    
    public void setIssuersRequired(boolean issuersRequired) {
        this.issuersRequired = issuersRequired;
    }
    
    public boolean isEnforceAllowedCallers() {
        return enforceAllowedCallers;
    }
    
    public void setEnforceAllowedCallers(boolean enforceAllowedCallers) {
        this.enforceAllowedCallers = enforceAllowedCallers;
    }
    
    public List<String> getAllowedCallers() {
        return allowedCallers;
    }
    
    public boolean isRequireSsl() {
        return requireSsl;
    }
    
    public void setRequireSsl(boolean requireSsl) {
        this.requireSsl = requireSsl;
    }
    
    public void setAllowedCallers(@NotNull List<String> allowedCallers) {
        this.allowedCallers = allowedCallers;
    }
    
    public List<String> getManagerRoles() {
        return managerRoles;
    }
    
    public void setManagerRoles(List<String> managerRoles) {
        this.managerRoles = managerRoles;
    }
    
    public String getDeniedAccessRole() {
        return deniedAccessRole;
    }
    
    public void setDeniedAccessRole(String deniedAccessRole) {
        this.deniedAccessRole = deniedAccessRole;
    }
    
    public Set<String> getRequiredRoles() {
        return requiredRoles;
    }
    
    public void setRequiredRoles(Set<String> requiredRoles) {
        this.requiredRoles = requiredRoles;
    }
    
    public Jwt getJwt() {
        return jwt;
    }
    
    public static class Jwt {
        private boolean enabled = true;
        private int ttl;
        
        public boolean isEnabled() {
            return enabled;
        }
        
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
        
        public int getTtl() {
            return ttl;
        }
        
        public long getTtl(TimeUnit timeUnit) {
            return timeUnit.convert(ttl, TimeUnit.SECONDS);
        }
        
        public void setTtl(int ttl) {
            this.ttl = ttl;
        }
    }
}
