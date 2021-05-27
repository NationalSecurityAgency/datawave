package datawave.microservice.query.config;

import java.util.concurrent.TimeUnit;

public class QueryExpirationProperties {
    private long idleTimeout = 15;
    private TimeUnit idleTimeUnit = TimeUnit.MINUTES;
    private long callTimeout = 60;
    private TimeUnit callTimeUnit = TimeUnit.MINUTES;
    private long shortCircuitCheckTime = callTimeout / 2;
    private TimeUnit shortCircuitCheckTimeUnit = TimeUnit.MINUTES;
    private long shortCircuitTimeout = Math.round(0.97 * callTimeout);
    private TimeUnit shortCircuitTimeUnit = TimeUnit.MINUTES;
    
    public long getIdleTimeout() {
        return idleTimeout;
    }
    
    public long getIdleTimeoutMillis() {
        return idleTimeUnit.toMillis(idleTimeout);
    }
    
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }
    
    public TimeUnit getIdleTimeUnit() {
        return idleTimeUnit;
    }
    
    public void setIdleTimeUnit(TimeUnit idleTimeUnit) {
        this.idleTimeUnit = idleTimeUnit;
    }
    
    public long getCallTimeout() {
        return callTimeout;
    }
    
    public long getCallTimeoutMillis() {
        return callTimeUnit.toMillis(callTimeout);
    }
    
    public void setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
    }
    
    public TimeUnit getCallTimeUnit() {
        return callTimeUnit;
    }
    
    public void setCallTimeUnit(TimeUnit callTimeUnit) {
        this.callTimeUnit = callTimeUnit;
    }
    
    public long getShortCircuitCheckTime() {
        return shortCircuitCheckTime;
    }
    
    public long getShortCircuitCheckTimeMillis() {
        return shortCircuitCheckTimeUnit.toMillis(shortCircuitCheckTime);
    }
    
    public void setShortCircuitCheckTime(long shortCircuitCheckTime) {
        this.shortCircuitCheckTime = shortCircuitCheckTime;
    }
    
    public TimeUnit getShortCircuitCheckTimeUnit() {
        return shortCircuitCheckTimeUnit;
    }
    
    public void setShortCircuitCheckTimeUnit(TimeUnit shortCircuitCheckTimeUnit) {
        this.shortCircuitCheckTimeUnit = shortCircuitCheckTimeUnit;
    }
    
    public long getShortCircuitTimeout() {
        return shortCircuitTimeout;
    }
    
    public long getShortCircuitTimeoutMillis() {
        return shortCircuitTimeUnit.toMillis(shortCircuitTimeout);
    }
    
    public void setShortCircuitTimeout(long shortCircuitTimeout) {
        this.shortCircuitTimeout = shortCircuitTimeout;
    }
    
    public TimeUnit getShortCircuitTimeUnit() {
        return shortCircuitTimeUnit;
    }
    
    public void setShortCircuitTimeUnit(TimeUnit shortCircuitTimeUnit) {
        this.shortCircuitTimeUnit = shortCircuitTimeUnit;
    }
}
