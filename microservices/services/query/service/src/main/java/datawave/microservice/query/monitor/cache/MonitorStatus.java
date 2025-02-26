package datawave.microservice.query.monitor.cache;

import java.io.Serializable;

public class MonitorStatus implements Serializable {
    private static final long serialVersionUID = -4850889280187199993L;
    
    private long lastCheckedMillis;
    
    public long getLastCheckedMillis() {
        return lastCheckedMillis;
    }
    
    public void setLastChecked(long lastCheckedMillis) {
        this.lastCheckedMillis = lastCheckedMillis;
    }
    
    public boolean isExpired(long currentTimeMillis, long expirationTimeoutMillis) {
        return (currentTimeMillis - lastCheckedMillis) >= expirationTimeoutMillis;
    }
}
