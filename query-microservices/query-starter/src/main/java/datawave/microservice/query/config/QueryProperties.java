package datawave.microservice.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import javax.validation.constraints.PositiveOrZero;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static datawave.microservice.query.QueryParameters.QUERY_EXPIRATION;
import static datawave.microservice.query.QueryParameters.QUERY_MAX_RESULTS_OVERRIDE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGESIZE;
import static datawave.microservice.query.QueryParameters.QUERY_PAGETIMEOUT;

@Validated
@ConfigurationProperties(prefix = "datawave.query")
public class QueryProperties {
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
    private List<String> updatableParams = Arrays.asList(QUERY_EXPIRATION, QUERY_PAGESIZE, QUERY_PAGETIMEOUT, QUERY_MAX_RESULTS_OVERRIDE);
    // Whether or not to wait for an executor create response before returning to the caller
    private boolean awaitExecutorCreateResponse = true;
    
    private QueryExpirationProperties expiration = new QueryExpirationProperties();
    private NextCallProperties nextCall = new NextCallProperties();
    private DefaultParameterProperties defaultParams = new DefaultParameterProperties();
    
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
}
