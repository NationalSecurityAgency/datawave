package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnegative;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;

@Validated
public class QueryProperties {
    
    @Valid
    private QueryExpirationProperties expiration;
    
    @NotEmpty
    private String privilegedRole = "PrivilegedUser";
    
    private DefaultParameters defaultParams = new DefaultParameters();
    
    public QueryExpirationProperties getExpiration() {
        return expiration;
    }
    
    public void setExpiration(QueryExpirationProperties expiration) {
        this.expiration = expiration;
    }
    
    public String getPrivilegedRole() {
        return privilegedRole;
    }
    
    public void setPrivilegedRole(String privilegedRole) {
        this.privilegedRole = privilegedRole;
    }
    
    public DefaultParameters getDefaultParams() {
        return defaultParams;
    }
    
    public void setDefaultParams(DefaultParameters defaultParams) {
        this.defaultParams = defaultParams;
    }
    
    @Validated
    public static class DefaultParameters {
        
        @NotEmpty
        private String pool = "unassigned";
        
        @Nonnegative
        private int maxConcurrentTasks = 10;
        
        public String getPool() {
            return pool;
        }
        
        public void setPool(String pool) {
            this.pool = pool;
        }
        
        public int getMaxConcurrentTasks() {
            return maxConcurrentTasks;
        }
        
        public void setMaxConcurrentTasks(int maxConcurrentTasks) {
            this.maxConcurrentTasks = maxConcurrentTasks;
        }
    }
}
