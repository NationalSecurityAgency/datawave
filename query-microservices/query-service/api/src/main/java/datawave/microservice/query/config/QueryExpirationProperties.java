package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import java.util.concurrent.TimeUnit;

@Validated
public class QueryExpirationProperties {
    @Positive
    private long idleTimeout = 15;
    @NotNull
    private TimeUnit idleTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long callTimeout = 60;
    @NotNull
    private TimeUnit callTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long pageMinTimeout = 1;
    @NotNull
    private TimeUnit pageMinTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long pageMaxTimeout = 60;
    @NotNull
    private TimeUnit pageMaxTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long shortCircuitCheckTime = callTimeout / 2;
    @NotNull
    private TimeUnit shortCircuitCheckTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long shortCircuitTimeout = Math.round(0.97 * callTimeout);
    @NotNull
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
    
    public long getPageMinTimeout() {
        return pageMinTimeout;
    }
    
    public long getPageMinTimeoutMillis() {
        return pageMinTimeUnit.toMillis(pageMinTimeout);
    }
    
    public void setPageMinTimeout(long pageMinTimeout) {
        this.pageMinTimeout = pageMinTimeout;
    }
    
    public TimeUnit getPageMinTimeUnit() {
        return pageMinTimeUnit;
    }
    
    public void setPageMinTimeUnit(TimeUnit pageMinTimeUnit) {
        this.pageMinTimeUnit = pageMinTimeUnit;
    }
    
    public long getPageMaxTimeout() {
        return pageMaxTimeout;
    }
    
    public long getPageMaxTimeoutMillis() {
        return pageMaxTimeUnit.toMillis(pageMaxTimeout);
    }
    
    public void setPageMaxTimeout(long pageMaxTimeout) {
        this.pageMaxTimeout = pageMaxTimeout;
    }
    
    public TimeUnit getPageMaxTimeUnit() {
        return pageMaxTimeUnit;
    }
    
    public void setPageMaxTimeUnit(TimeUnit pageMaxTimeUnit) {
        this.pageMaxTimeUnit = pageMaxTimeUnit;
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
