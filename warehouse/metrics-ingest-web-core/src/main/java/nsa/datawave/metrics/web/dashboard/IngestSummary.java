package nsa.datawave.metrics.web.dashboard;

public class IngestSummary {
    
    private final long dateTime;
    private long liveEventCount;
    private long liveAveTime;
    private long liveEventRate;
    private long liveAvePollerTime;
    private long liveAveIngestWaitTime;
    private long liveAveIngestTime;
    private long bulkEventCount;
    private long bulkAveTime;
    private long bulkEventRate;
    private long bulkAvePollerTime;
    private long bulkAveIngestWaitTime;
    private long bulkAveIngestTime;
    private long bulkAveLoaderWaitTime;
    private long bulkAveLoaderTime;
    
    /**
     * Default to all values being 0.
     *
     * @param dateTime
     */
    public IngestSummary(long dateTime) {
        this.dateTime = dateTime;
    }
    
    public long getDateTime() {
        return dateTime;
    }
    
    public long getLiveEventCount() {
        return liveEventCount;
    }
    
    public long getLiveAveTime() {
        return liveAveTime;
    }
    
    public long getLiveEventRate() {
        return liveEventRate;
    }
    
    public long getLiveAvePollerTime() {
        return liveAvePollerTime;
    }
    
    public long getLiveAveIngestWaitTime() {
        return liveAveIngestWaitTime;
    }
    
    public long getLiveAveIngestTime() {
        return liveAveIngestTime;
    }
    
    public long getBulkEventCount() {
        return bulkEventCount;
    }
    
    public long getBulkAveTime() {
        return bulkAveTime;
    }
    
    public long getBulkEventRate() {
        return bulkEventRate;
    }
    
    public long getBulkAvePollerTime() {
        return bulkAvePollerTime;
    }
    
    public long getBulkAveIngestWaitTime() {
        return bulkAveIngestWaitTime;
    }
    
    public long getBulkAveIngestTime() {
        return bulkAveIngestTime;
    }
    
    public long getBulkAveLoaderWaitTime() {
        return bulkAveLoaderWaitTime;
    }
    
    public long getBulkAveLoaderTime() {
        return bulkAveLoaderTime;
    }
    
    public void setLiveEventCount(long liveEventCount) {
        this.liveEventCount = liveEventCount;
    }
    
    public void setLiveAveTime(long liveAveTime) {
        this.liveAveTime = liveAveTime;
    }
    
    public void setLiveEventRate(long liveEventRate) {
        this.liveEventRate = liveEventRate;
    }
    
    public void setLiveAvePollerTime(long liveAvePollerTime) {
        this.liveAvePollerTime = liveAvePollerTime;
    }
    
    public void setLiveAveIngestWaitTime(long liveAveIngestWaitTime) {
        this.liveAveIngestWaitTime = liveAveIngestWaitTime;
    }
    
    public void setLiveAveIngestTime(long liveAveIngestTime) {
        this.liveAveIngestTime = liveAveIngestTime;
    }
    
    public void setBulkEventCount(long bulkEventCount) {
        this.bulkEventCount = bulkEventCount;
    }
    
    public void setBulkAveTime(long bulkAveTime) {
        this.bulkAveTime = bulkAveTime;
    }
    
    public void setBulkEventRate(long bulkEventRate) {
        this.bulkEventRate = bulkEventRate;
    }
    
    public void setBulkAvePollerTime(long bulkAvePollerTime) {
        this.bulkAvePollerTime = bulkAvePollerTime;
    }
    
    public void setBulkAveIngestWaitTime(long bulkAveIngestWaitTime) {
        this.bulkAveIngestWaitTime = bulkAveIngestWaitTime;
    }
    
    public void setBulkAveIngestTime(long bulkAveIngestTime) {
        this.bulkAveIngestTime = bulkAveIngestTime;
    }
    
    public void setBulkAveLoaderWaitTime(long bulkAveLoaderWaitTime) {
        this.bulkAveLoaderWaitTime = bulkAveLoaderWaitTime;
    }
    
    public void setBulkAveLoaderTime(long bulkAveLoaderTime) {
        this.bulkAveLoaderTime = bulkAveLoaderTime;
    }
}
