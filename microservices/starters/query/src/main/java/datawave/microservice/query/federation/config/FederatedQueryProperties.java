package datawave.microservice.query.federation.config;

import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;

public class FederatedQueryProperties {
    private String queryServiceUri = "https://query:8443/query/v1/query";
    
    private String queryMetricServiceUri = "https://querymetric:8443/querymetric/v1/id";
    
    // max bytes to buffer for each rest call (-1 is unlimited)
    private int maxBytesToBuffer = -1;
    
    @PositiveOrZero
    private long createTimeout = TimeUnit.SECONDS.toMillis(30);
    
    @NotNull
    private TimeUnit createTimeoutUnit = TimeUnit.MILLISECONDS;
    
    @PositiveOrZero
    private long nextTimeout = TimeUnit.HOURS.toMillis(1);
    
    @NotNull
    private TimeUnit nextTimeoutUnit = TimeUnit.MILLISECONDS;
    
    @PositiveOrZero
    private long closeTimeout = TimeUnit.HOURS.toMillis(1);
    
    @NotNull
    private TimeUnit closeTimeoutUnit = TimeUnit.MILLISECONDS;
    
    @PositiveOrZero
    private long planTimeout = TimeUnit.MINUTES.toMillis(20);
    
    @NotNull
    private TimeUnit planTimeoutUnit = TimeUnit.MILLISECONDS;
    
    public String getQueryServiceUri() {
        return queryServiceUri;
    }
    
    public void setQueryServiceUri(String queryServiceUri) {
        this.queryServiceUri = queryServiceUri;
    }
    
    public String getQueryMetricServiceUri() {
        return queryMetricServiceUri;
    }
    
    public void setQueryMetricServiceUri(String queryMetricServiceUri) {
        this.queryMetricServiceUri = queryMetricServiceUri;
    }
    
    public int getMaxBytesToBuffer() {
        return maxBytesToBuffer;
    }
    
    public void setMaxBytesToBuffer(int maxBytesToBuffer) {
        this.maxBytesToBuffer = maxBytesToBuffer;
    }
    
    public long getCreateTimeout() {
        return createTimeout;
    }
    
    public long getCreateTimeoutMillis() {
        return createTimeoutUnit.toMillis(createTimeout);
    }
    
    public void setCreateTimeout(long createTimeout) {
        this.createTimeout = createTimeout;
    }
    
    public TimeUnit getCreateTimeoutUnit() {
        return createTimeoutUnit;
    }
    
    public void setCreateTimeoutUnit(TimeUnit createTimeoutUnit) {
        this.createTimeoutUnit = createTimeoutUnit;
    }
    
    public long getNextTimeout() {
        return nextTimeout;
    }
    
    public long getNextTimeoutMillis() {
        return nextTimeoutUnit.toMillis(nextTimeout);
    }
    
    public void setNextTimeout(long nextTimeout) {
        this.nextTimeout = nextTimeout;
    }
    
    public TimeUnit getNextTimeoutUnit() {
        return nextTimeoutUnit;
    }
    
    public void setNextTimeoutUnit(TimeUnit nextTimeoutUnit) {
        this.nextTimeoutUnit = nextTimeoutUnit;
    }
    
    public long getCloseTimeout() {
        return closeTimeout;
    }
    
    public long getCloseTimeoutMillis() {
        return closeTimeoutUnit.toMillis(closeTimeout);
    }
    
    public void setCloseTimeout(long closeTimeout) {
        this.closeTimeout = closeTimeout;
    }
    
    public TimeUnit getCloseTimeoutUnit() {
        return closeTimeoutUnit;
    }
    
    public void setCloseTimeoutUnit(TimeUnit closeTimeoutUnit) {
        this.closeTimeoutUnit = closeTimeoutUnit;
    }
    
    public long getPlanTimeout() {
        return planTimeout;
    }
    
    public long getPlanTimeoutMillis() {
        return planTimeoutUnit.toMillis(planTimeout);
    }
    
    public void setPlanTimeout(long planTimeout) {
        this.planTimeout = planTimeout;
    }
    
    public TimeUnit getPlanTimeoutUnit() {
        return planTimeoutUnit;
    }
    
    public void setPlanTimeoutUnit(TimeUnit planTimeoutUnit) {
        this.planTimeoutUnit = planTimeoutUnit;
    }
}
