package datawave.microservice.query.config;

import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGETIMEOUT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.query")
public class QueryProperties {
    private List<String> adminRoles = Arrays.asList("Administrator", "JBossAdministrator");
    @NotEmpty
    private String privilegedRole = "PrivilegedUser";
    // The amount of time to wait for the lock to be acquired
    @PositiveOrZero
    private long lockWaitTime = TimeUnit.SECONDS.toMillis(5);
    @NotNull
    private TimeUnit lockWaitTimeUnit = TimeUnit.MILLISECONDS;
    // The amount of time that the lock will be held before being automatically released
    @Positive
    private long lockLeaseTime = TimeUnit.SECONDS.toMillis(30);
    @NotNull
    private TimeUnit lockLeaseTimeUnit = TimeUnit.MILLISECONDS;
    @NotEmpty
    private String queryServiceName = "query";
    @NotEmpty
    private String executorServiceName = "executor";
    // These are the only parameters that can be updated for a running query
    private List<String> updatableParams = Arrays.asList(QUERY_PAGESIZE, QUERY_PAGETIMEOUT, QUERY_MAX_RESULTS_OVERRIDE);
    // Whether or not to wait for an executor create response before returning to the caller
    private boolean awaitExecutorCreateResponse = true;
    
    private QueryExpirationProperties expiration = new QueryExpirationProperties();
    private NextCallProperties nextCall = new NextCallProperties();
    private DefaultParameterProperties defaultParams = new DefaultParameterProperties();
    private String poolOverride = null;
    private String poolHeader = "Pool";
    
    // here we're breaking this out by executor pool
    private Map<String,PoolProperties> poolLimits = new HashMap<>();
    
    public List<String> getAdminRoles() {
        return adminRoles;
    }
    
    public void setAdminRoles(List<String> adminRoles) {
        this.adminRoles = adminRoles;
    }
    
    public String getPrivilegedRole() {
        return privilegedRole;
    }
    
    public void setPrivilegedRole(String privilegedRole) {
        this.privilegedRole = privilegedRole;
    }
    
    public long getLockWaitTime() {
        return lockWaitTime;
    }
    
    public long getLockWaitTimeMillis() {
        return lockWaitTimeUnit.toMillis(lockWaitTime);
    }
    
    public void setLockWaitTime(long lockWaitTime) {
        this.lockWaitTime = lockWaitTime;
    }
    
    public TimeUnit getLockWaitTimeUnit() {
        return lockWaitTimeUnit;
    }
    
    public void setLockWaitTimeUnit(TimeUnit lockWaitTimeUnit) {
        this.lockWaitTimeUnit = lockWaitTimeUnit;
    }
    
    public long getLockLeaseTime() {
        return lockLeaseTime;
    }
    
    public long getLockLeaseTimeMillis() {
        return lockLeaseTimeUnit.toMillis(lockLeaseTime);
    }
    
    public void setLockLeaseTime(long lockLeaseTime) {
        this.lockLeaseTime = lockLeaseTime;
    }
    
    public TimeUnit getLockLeaseTimeUnit() {
        return lockLeaseTimeUnit;
    }
    
    public void setLockLeaseTimeUnit(TimeUnit lockLeaseTimeUnit) {
        this.lockLeaseTimeUnit = lockLeaseTimeUnit;
    }
    
    public String getQueryServiceName() {
        return queryServiceName;
    }
    
    public void setQueryServiceName(String queryServiceName) {
        this.queryServiceName = queryServiceName;
    }
    
    public String getExecutorServiceName() {
        return executorServiceName;
    }
    
    public void setExecutorServiceName(String executorServiceName) {
        this.executorServiceName = executorServiceName;
    }
    
    public List<String> getUpdatableParams() {
        return updatableParams;
    }
    
    public void setUpdatableParams(List<String> updatableParams) {
        this.updatableParams = updatableParams;
    }
    
    public boolean isAwaitExecutorCreateResponse() {
        return awaitExecutorCreateResponse;
    }
    
    public void setAwaitExecutorCreateResponse(boolean awaitExecutorCreateResponse) {
        this.awaitExecutorCreateResponse = awaitExecutorCreateResponse;
    }
    
    public QueryExpirationProperties getExpiration() {
        return expiration;
    }
    
    public void setExpiration(QueryExpirationProperties expiration) {
        this.expiration = expiration;
    }
    
    public NextCallProperties getNextCall() {
        return nextCall;
    }
    
    public void setNextCall(NextCallProperties nextCall) {
        this.nextCall = nextCall;
    }
    
    public DefaultParameterProperties getDefaultParams() {
        return defaultParams;
    }
    
    public void setDefaultParams(DefaultParameterProperties defaultParams) {
        this.defaultParams = defaultParams;
    }
    
    public String getPoolOverride() {
        return poolOverride;
    }
    
    public void setPoolOverride(String poolOverride) {
        this.poolOverride = poolOverride;
    }
    
    public String getPoolHeader() {
        return poolHeader;
    }
    
    public void setPoolHeader(String poolHeader) {
        this.poolHeader = poolHeader;
    }
    
    public Map<String,PoolProperties> getPoolLimits() {
        return poolLimits;
    }
    
    public void setPoolLimits(Map<String,PoolProperties> poolLimits) {
        this.poolLimits = poolLimits;
    }
    
    public static class PoolProperties {
        // this is the max queries PER executor, keyed by connection factory pool
        private Map<String,Integer> maxQueriesPerExecutor = new LinkedHashMap<>();
        
        // How long until we consider an executor to be dead
        private long livenessTimeout = TimeUnit.SECONDS.toMillis(90);
        private TimeUnit livenessTimeoutUnit = TimeUnit.MILLISECONDS;
        
        public Map<String,Integer> getMaxQueriesPerExecutor() {
            return maxQueriesPerExecutor;
        }
        
        public void setMaxQueriesPerExecutor(Map<String,Integer> maxQueriesPerExecutor) {
            this.maxQueriesPerExecutor = maxQueriesPerExecutor;
        }
        
        public long getLivenessTimeout() {
            return livenessTimeout;
        }
        
        public long getLivenessTimeoutMillis() {
            return livenessTimeoutUnit.toMillis(livenessTimeout);
        }
        
        public void setLivenessTimeout(long livenessTimeout) {
            this.livenessTimeout = livenessTimeout;
        }
        
        public TimeUnit getLivenessTimeoutUnit() {
            return livenessTimeoutUnit;
        }
        
        public void setLivenessTimeoutUnit(TimeUnit livenessTimeoutUnit) {
            this.livenessTimeoutUnit = livenessTimeoutUnit;
        }
    }
}
