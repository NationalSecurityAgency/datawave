package datawave.microservice.query.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

import org.springframework.validation.annotation.Validated;

@Validated
public class QueryExpirationProperties {
    @Positive
    private long idleTimeout = 15;
    @NotNull
    private TimeUnit idleTimeoutUnit = TimeUnit.MINUTES;
    @Positive
    private long progressTimeout = 5;
    @NotNull
    private TimeUnit progressTimeoutUnit = TimeUnit.MINUTES;
    @Positive
    private long callTimeout = 60;
    @NotNull
    private TimeUnit callTimeoutUnit = TimeUnit.MINUTES;
    @Positive
    private long callTimeoutInterval = 1;
    @NotNull
    private TimeUnit callTimeoutIntervalUnit = TimeUnit.MINUTES;
    @Positive
    private long pageMinTimeout = 1;
    @NotNull
    private TimeUnit pageMinTimeoutUnit = TimeUnit.MINUTES;
    @Positive
    private long pageMaxTimeout = 60;
    @NotNull
    private TimeUnit pageMaxTimeoutUnit = TimeUnit.MINUTES;
    @Positive
    private long shortCircuitCheckTime = callTimeout / 2;
    @NotNull
    private TimeUnit shortCircuitCheckTimeUnit = TimeUnit.MINUTES;
    @Positive
    private long shortCircuitTimeout = Math.round(0.97 * callTimeout);
    @NotNull
    private TimeUnit shortCircuitTimeoutUnit = TimeUnit.MINUTES;
    
    @Positive
    @Deprecated // to be replaced by the long running query timeout
    private int maxLongRunningTimeoutRetries = 3;
    
    @Positive
    private long longRunningQueryTimeout = (maxLongRunningTimeoutRetries + 1) * callTimeoutUnit.toMinutes(callTimeout);
    
    @NotNull
    private TimeUnit longRunningQueryTimeoutUnit = TimeUnit.MINUTES;
    
    public long getIdleTimeout() {
        return idleTimeout;
    }
    
    public long getIdleTimeoutMillis() {
        return idleTimeoutUnit.toMillis(idleTimeout);
    }
    
    public void setIdleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
    }
    
    public TimeUnit getIdleTimeoutUnit() {
        return idleTimeoutUnit;
    }
    
    public void setIdleTimeoutUnit(TimeUnit idleTimeoutUnit) {
        this.idleTimeoutUnit = idleTimeoutUnit;
    }
    
    public long getProgressTimeout() {
        return progressTimeout;
    }
    
    public long getProgressTimeoutMillis() {
        return progressTimeoutUnit.toMillis(progressTimeout);
    }
    
    public void setProgressTimeout(long progressTimeout) {
        this.progressTimeout = progressTimeout;
    }
    
    public TimeUnit getProgressTimeoutUnit() {
        return progressTimeoutUnit;
    }
    
    public void setProgressTimeoutUnit(TimeUnit progressTimeoutUnit) {
        this.progressTimeoutUnit = progressTimeoutUnit;
    }
    
    public long getCallTimeout() {
        return callTimeout;
    }
    
    public long getCallTimeoutMillis() {
        return callTimeoutUnit.toMillis(callTimeout);
    }
    
    public void setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
    }
    
    public TimeUnit getCallTimeoutUnit() {
        return callTimeoutUnit;
    }
    
    public void setCallTimeoutUnit(TimeUnit callTimeoutUnit) {
        this.callTimeoutUnit = callTimeoutUnit;
    }
    
    public long getCallTimeoutInterval() {
        return callTimeoutInterval;
    }
    
    public long getCallTimeoutIntervalMillis() {
        return callTimeoutIntervalUnit.toMillis(callTimeoutInterval);
    }
    
    public void setCallTimeoutInterval(long callTimeoutInterval) {
        this.callTimeoutInterval = callTimeoutInterval;
    }
    
    public TimeUnit getCallTimeoutIntervalUnit() {
        return callTimeoutIntervalUnit;
    }
    
    public void setCallTimeoutIntervalUnit(TimeUnit callTimeoutIntervalUnit) {
        this.callTimeoutIntervalUnit = callTimeoutIntervalUnit;
    }
    
    public long getPageMinTimeout() {
        return pageMinTimeout;
    }
    
    public long getPageMinTimeoutMillis() {
        return pageMinTimeoutUnit.toMillis(pageMinTimeout);
    }
    
    public void setPageMinTimeout(long pageMinTimeout) {
        this.pageMinTimeout = pageMinTimeout;
    }
    
    public TimeUnit getPageMinTimeoutUnit() {
        return pageMinTimeoutUnit;
    }
    
    public void setPageMinTimeoutUnit(TimeUnit pageMinTimeoutUnit) {
        this.pageMinTimeoutUnit = pageMinTimeoutUnit;
    }
    
    public long getPageMaxTimeout() {
        return pageMaxTimeout;
    }
    
    public long getPageMaxTimeoutMillis() {
        return pageMaxTimeoutUnit.toMillis(pageMaxTimeout);
    }
    
    public void setPageMaxTimeout(long pageMaxTimeout) {
        this.pageMaxTimeout = pageMaxTimeout;
    }
    
    public TimeUnit getPageMaxTimeoutUnit() {
        return pageMaxTimeoutUnit;
    }
    
    public void setPageMaxTimeoutUnit(TimeUnit pageMaxTimeoutUnit) {
        this.pageMaxTimeoutUnit = pageMaxTimeoutUnit;
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
        return shortCircuitTimeoutUnit.toMillis(shortCircuitTimeout);
    }
    
    public void setShortCircuitTimeout(long shortCircuitTimeout) {
        this.shortCircuitTimeout = shortCircuitTimeout;
    }
    
    public TimeUnit getShortCircuitTimeoutUnit() {
        return shortCircuitTimeoutUnit;
    }
    
    public void setShortCircuitTimeoutUnit(TimeUnit shortCircuitTimeoutUnit) {
        this.shortCircuitTimeoutUnit = shortCircuitTimeoutUnit;
    }
    
    public int getMaxLongRunningTimeoutRetries() {
        return maxLongRunningTimeoutRetries;
    }
    
    public void setMaxLongRunningTimeoutRetries(int maxLongRunningTimeoutRetries) {
        this.maxLongRunningTimeoutRetries = maxLongRunningTimeoutRetries;
    }
    
    public long getLongRunningQueryTimeout() {
        return longRunningQueryTimeout;
    }
    
    public void setLongRunningQueryTimeout(long longRunningQueryTimeout) {
        this.longRunningQueryTimeout = longRunningQueryTimeout;
    }
    
    public TimeUnit getLongRunningQueryTimeoutUnit() {
        return longRunningQueryTimeoutUnit;
    }
    
    public void setLongRunningQueryTimeoutUnit(TimeUnit longRunningQueryTimeoutUnit) {
        this.longRunningQueryTimeoutUnit = longRunningQueryTimeoutUnit;
    }
    
    public long getLongRunningQueryTimeoutMillis() {
        return longRunningQueryTimeoutUnit.toMillis(longRunningQueryTimeout);
    }
}
