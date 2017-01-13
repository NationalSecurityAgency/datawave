package nsa.datawave.webservice.query.cache;

import nsa.datawave.webservice.query.runner.RunningQuery.RunningQueryTiming;

public class RunningQueryTimingImpl implements RunningQueryTiming {
    // The max time allowed within a call (e.g. next())
    private long maxCallMs = 60 * 60 * 1000;
    // The time after which we start checking the page size velocity
    private long pageSizeShortCircuitCheckTimeMs = 30 * 60 * 1000;
    // The time after which will we prematurely return if we have results.
    private long pageShortCircuitTimeoutMs = 58 * 60 * 1000;
    
    public RunningQueryTimingImpl(QueryExpirationConfiguration conf) {
        this(conf.getCallTimeInMS(), conf.getPageSizeShortCircuitCheckTimeInMS(), conf.getPageShortCircuitTimeoutInMS());
    }
    
    public RunningQueryTimingImpl(long maxCallMs, long pageSizeShortCircuitCheckTimeMs, long pageShortCircuitTimeoutMs) {
        this.maxCallMs = maxCallMs;
        this.pageSizeShortCircuitCheckTimeMs = pageSizeShortCircuitCheckTimeMs;
        this.pageShortCircuitTimeoutMs = pageShortCircuitTimeoutMs;
    }
    
    public long getMaxCallMs() {
        return maxCallMs;
    }
    
    public long getPageSizeShortCircuitCheckTimeMs() {
        return pageSizeShortCircuitCheckTimeMs;
    }
    
    public long getPageShortCircuitTimeoutMs() {
        return pageShortCircuitTimeoutMs;
    }
    
    @Override
    public boolean shoudReturnPartialResults(int pageSize, int maxPageSize, long timeInCall) {
        
        // only return prematurely if we have at least 1 result
        if (pageSize > 0) {
            
            // if after the page size short circuit check time
            if (timeInCall >= pageSizeShortCircuitCheckTimeMs) {
                float percentTimeComplete = (float) timeInCall / (float) (this.maxCallMs);
                float percentResultsComplete = (float) pageSize / (float) maxPageSize;
                // if the percent results complete is less than the percent time complete, then break out
                if (percentResultsComplete < percentTimeComplete) {
                    return true;
                }
            }
            
            // if after the page short circuit timeout, then break out
            if (timeInCall >= this.pageShortCircuitTimeoutMs) {
                return true;
            }
            
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("maxCallMs=").append(maxCallMs).append(" ; ");
        builder.append("pageSizeShortCircuitCheckTimeMs=").append(pageSizeShortCircuitCheckTimeMs);
        builder.append("pageShortCircuitTimeoutMs=").append(pageShortCircuitTimeoutMs).append(" ; ");
        return builder.toString();
    }
}
