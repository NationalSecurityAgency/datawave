package datawave.ingest.util.cache.lease;

import java.util.Collection;
import java.util.Objects;

public class LockCacheStatus {
    private final boolean cacheActive;
    private final boolean cacheLocked;
    private final Collection<String> jobIds;
    
    public LockCacheStatus(boolean cacheLocked, Collection<String> jobIds) {
        this.cacheLocked = cacheLocked;
        this.cacheActive = !jobIds.isEmpty();
        this.jobIds = jobIds;
    }
    
    public boolean isCacheActive() {
        return cacheActive;
    }
    
    public boolean isCacheLocked() {
        return cacheLocked;
    }
    
    public Collection<String> getJobIds() {
        return jobIds;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        
        LockCacheStatus that = (LockCacheStatus) obj;
        return this.cacheActive == that.cacheActive && this.cacheLocked == that.cacheLocked && (this.jobIds.equals(that.jobIds));
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(cacheActive, cacheLocked, jobIds);
    }
}
