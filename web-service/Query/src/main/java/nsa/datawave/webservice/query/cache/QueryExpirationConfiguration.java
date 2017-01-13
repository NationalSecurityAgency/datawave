package nsa.datawave.webservice.query.cache;

import nsa.datawave.configuration.RefreshableScope;

/**
 * Configuration file is located at: datawave/query/QueryExpiration.xml
 */
@RefreshableScope
public class QueryExpirationConfiguration {
    
    private long idleTimeMinutes = 15;
    private long pageSizeShortCircuitCheckTimeMinutes = 15;
    private long pageShortCircuitTimeoutMinutes = 28;
    private long callTimeMinutes = 30;
    
    public long getIdleTimeMinutes() {
        return idleTimeMinutes;
    }
    
    public long getIdleTimeInMS() {
        return idleTimeMinutes * 60 * 1000;
    }
    
    public void setIdleTime(long idleTimeMinutes) {
        this.idleTimeMinutes = idleTimeMinutes;
    }
    
    public void setIdleTimeMinutes(long idleTimeMinutes) {
        this.idleTimeMinutes = idleTimeMinutes;
    }
    
    public long getCallTimeMinutes() {
        return callTimeMinutes;
    }
    
    public long getCallTimeInMS() {
        return callTimeMinutes * 60 * 1000;
    }
    
    public void setCallTime(long callTimeMinutes) {
        this.callTimeMinutes = callTimeMinutes;
    }
    
    public void setCallTimeMinutes(long callTimeMinutes) {
        this.callTimeMinutes = callTimeMinutes;
    }
    
    public float getPageSizeShortCircuitCheckTimeMinutes() {
        return pageSizeShortCircuitCheckTimeMinutes;
    }
    
    public long getPageSizeShortCircuitCheckTimeInMS() {
        return pageSizeShortCircuitCheckTimeMinutes * 60 * 1000;
    }
    
    public void setPageSizeShortCircuitCheckTime(long pageSizeShortCircuitCheckTimeMinutes) {
        this.pageSizeShortCircuitCheckTimeMinutes = pageSizeShortCircuitCheckTimeMinutes;
    }
    
    public void setPageSizeShortCircuitCheckTimeMinutes(long pageSizeShortCircuitCheckTimeMinutes) {
        this.pageSizeShortCircuitCheckTimeMinutes = pageSizeShortCircuitCheckTimeMinutes;
    }
    
    public long getPageShortCircuitTimeoutMinutes() {
        return pageShortCircuitTimeoutMinutes;
    }
    
    public long getPageShortCircuitTimeoutInMS() {
        return pageShortCircuitTimeoutMinutes * 60 * 1000;
    }
    
    public void setPageShortCircuitTimeout(long pageShortCircuitTimeoutMinutes) {
        this.pageShortCircuitTimeoutMinutes = pageShortCircuitTimeoutMinutes;
    }
    
    public void setPageShortCircuitTimeoutMinutes(long pageShortCircuitTimeoutMinutes) {
        this.pageShortCircuitTimeoutMinutes = pageShortCircuitTimeoutMinutes;
    }
    
}
