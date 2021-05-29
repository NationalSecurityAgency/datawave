package datawave.microservice.query.monitor.cache;

public class MonitorStatus {
    private long lastCheckedMillis;
    
    public long getLastCheckedMillis() {
        return lastCheckedMillis;
    }
    
    public void setLastChecked(long lastCheckedMillis) {
        this.lastCheckedMillis = lastCheckedMillis;
    }
}
