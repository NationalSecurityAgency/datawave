package datawave.microservice.authorization.config;

import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Datawave-specific extensions to {@link SecurityProperties}
 */
@ConfigurationProperties(prefix = "security")
public class DatawaveSecurityProperties extends SecurityProperties {
    @NestedConfigurationProperty
    private final Jwt jwt = new Jwt();
    private boolean proxiedEntitiesRequired;
    private boolean issuersRequired;
    private boolean enforceAllowedCallers = true;
    private List<String> allowedCallers = new ArrayList<>();
    
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
    
    public void setAllowedCallers(@NotNull List<String> allowedCallers) {
        this.allowedCallers = allowedCallers;
    }
    
    public Jwt getJwt() {
        return jwt;
    }
    
    public static class Jwt {
        private int ttl;
        
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
