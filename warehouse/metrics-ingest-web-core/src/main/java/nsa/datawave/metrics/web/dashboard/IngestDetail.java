package nsa.datawave.metrics.web.dashboard;

/**
 * Representation of live and bulk ingest metrics.
 */
public class IngestDetail implements Comparable<IngestDetail> {
    
    private final long timestamp;
    private final String dataType;
    private final boolean live;
    private final long eventCount;
    private final long pollerTime;
    private final long ingestWaitTime;
    private final long ingestTime;
    private final long loaderWaitTime;
    private final long loaderTime;
    
    public IngestDetail(long timestamp, String dataType, String ingestType, long eventCount, long pollerTime, long ingestWaitTime, long ingestTime,
                    long loaderWaitTime, long loaderTime) {
        this.timestamp = timestamp;
        this.dataType = dataType;
        this.live = ingestType.equals("live") ? true : false;
        this.eventCount = eventCount;
        this.pollerTime = pollerTime / 1000;
        this.ingestWaitTime = ingestWaitTime / 1000;
        this.ingestTime = ingestTime / 1000;
        this.loaderWaitTime = loaderWaitTime / 1000;
        this.loaderTime = loaderTime / 1000;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getDataType() {
        return dataType;
    }
    
    public boolean isLive() {
        return live;
    }
    
    public long getEventCount() {
        return eventCount;
    }
    
    public long getPollerTime() {
        return pollerTime;
    }
    
    public long getIngestWaitTime() {
        return ingestWaitTime;
    }
    
    public long getIngestTime() {
        return ingestTime;
    }
    
    public long getLoaderWaitTime() {
        return loaderWaitTime;
    }
    
    public long getLoaderTime() {
        return loaderTime;
    }
    
    @Override
    public int compareTo(IngestDetail that) {
        return new Long(this.getTimestamp()).compareTo(new Long(that.getTimestamp()));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IngestDetail)) {
            return false;
        }
        IngestDetail that = (IngestDetail) obj;
        return Long.valueOf(this.getTimestamp()).equals(Long.valueOf(that.getTimestamp()));
    }
    
    @Override
    public int hashCode() {
        return Long.valueOf(this.getTimestamp()).hashCode();
    }
}
