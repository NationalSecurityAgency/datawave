package datawave.webservice.query.cache;

import java.io.Serializable;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricFactory;

/**
 * getLastUsed() on Infinispan Cache entry objects is probably used for eviction from the L1 cache and is therefore unreliable. This class will be used as the
 * base class for all objects put into the query cache.
 *
 */
public abstract class AbstractRunningQuery implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    private volatile long lastUsed = 0;
    private volatile long timeOfCurrentCall = 0;
    private volatile boolean activeCall = false;
    
    protected BaseQueryMetric metric;
    
    protected AbstractRunningQuery(QueryMetricFactory metricFactory) {
        if (null == metricFactory) {
            throw new IllegalArgumentException("metricFactory cannot be null");
        }
        touch();
        metric = metricFactory.createMetric();
    }
    
    public long getLastUsed() {
        return lastUsed;
    }
    
    public void touch() {
        this.lastUsed = System.currentTimeMillis();
    }
    
    public BaseQueryMetric getMetric() {
        return this.metric;
    }
    
    public void setMetric(BaseQueryMetric metric) {
        this.metric = metric;
    }
    
    /**
     * 
     * @return page number of last page returned from the call to next()
     */
    public abstract long getLastPageNumber();
    
    public boolean hasActiveCall() {
        return activeCall;
    }
    
    public void setTimeOfCurrentCall(Long callTime) {
        timeOfCurrentCall = callTime;
    }
    
    public long getTimeOfCurrentCall() {
        return timeOfCurrentCall;
    }
    
    public void setActiveCall(boolean activeCall) {
        this.activeCall = activeCall;
        
        if (activeCall) {
            setTimeOfCurrentCall(System.currentTimeMillis());
        } else {
            setTimeOfCurrentCall(0L);
        }
    }
}
