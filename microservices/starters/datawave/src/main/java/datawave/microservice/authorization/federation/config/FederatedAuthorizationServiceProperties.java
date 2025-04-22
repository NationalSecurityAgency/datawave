package datawave.microservice.authorization.federation.config;

import java.util.concurrent.TimeUnit;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

public class FederatedAuthorizationServiceProperties {
    private String federatedAuthorizationUri = "https://authorization:8443/authorization/v2";
    
    // max bytes to buffer for each rest call (-1 is unlimited)
    private int maxBytesToBuffer = -1;
    
    @Valid
    private RetryTimeoutProperties listEffectiveAuthorizationsRetry = new RetryTimeoutProperties();
    
    @Valid
    private RetryTimeoutProperties flushCachedCredentialsRetry = new RetryTimeoutProperties();
    
    public String getFederatedAuthorizationUri() {
        return federatedAuthorizationUri;
    }
    
    public void setFederatedAuthorizationUri(String federatedAuthorizationUri) {
        this.federatedAuthorizationUri = federatedAuthorizationUri;
    }
    
    public int getMaxBytesToBuffer() {
        return maxBytesToBuffer;
    }
    
    public void setMaxBytesToBuffer(int maxBytesToBuffer) {
        this.maxBytesToBuffer = maxBytesToBuffer;
    }
    
    public RetryTimeoutProperties getListEffectiveAuthorizationsRetry() {
        return listEffectiveAuthorizationsRetry;
    }
    
    public void setListEffectiveAuthorizationsRetry(RetryTimeoutProperties listEffectiveAuthorizationsRetry) {
        this.listEffectiveAuthorizationsRetry = listEffectiveAuthorizationsRetry;
    }
    
    public RetryTimeoutProperties getFlushCachedCredentialsRetry() {
        return flushCachedCredentialsRetry;
    }
    
    public void setFlushCachedCredentialsRetry(RetryTimeoutProperties flushCachedCredentialsRetry) {
        this.flushCachedCredentialsRetry = flushCachedCredentialsRetry;
    }
    
    public class RetryTimeoutProperties {
        @PositiveOrZero
        private long timeout = TimeUnit.SECONDS.toMillis(30);
        
        @NotNull
        private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
        
        @PositiveOrZero
        private int retries = 5;
        
        @PositiveOrZero
        private long retryDelay = TimeUnit.SECONDS.toMillis(2);
        
        @NotNull
        private TimeUnit retryDelayUnit = TimeUnit.MILLISECONDS;
        
        public long getTimeout() {
            return timeout;
        }
        
        public long getTimeoutMillis() {
            return timeoutUnit.toMillis(timeout);
        }
        
        public void setTimeout(long timeout) {
            this.timeout = timeout;
        }
        
        public TimeUnit getTimeoutUnit() {
            return timeoutUnit;
        }
        
        public void setTimeoutUnit(TimeUnit timeoutUnit) {
            this.timeoutUnit = timeoutUnit;
        }
        
        public int getRetries() {
            return retries;
        }
        
        public void setRetries(int retries) {
            this.retries = retries;
        }
        
        public long getRetryDelay() {
            return retryDelay;
        }
        
        public long getRetryDelayMillis() {
            return retryDelayUnit.toMillis(retryDelay);
        }
        
        public void setRetryDelay(long retryDelay) {
            this.retryDelay = retryDelay;
        }
        
        public TimeUnit getRetryDelayUnit() {
            return retryDelayUnit;
        }
        
        public void setRetryDelayUnit(TimeUnit retryDelayUnit) {
            this.retryDelayUnit = retryDelayUnit;
        }
    }
}
