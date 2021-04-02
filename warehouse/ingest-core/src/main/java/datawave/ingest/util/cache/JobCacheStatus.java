package datawave.ingest.util.cache;

import datawave.ingest.util.cache.lease.LockCacheStatus;
import datawave.ingest.util.cache.path.FileSystemPath;

import java.util.Objects;

/** A job cache status is made up of its path and its lock attributes */
public class JobCacheStatus {
    
    private final LockCacheStatus lockCacheStatus;
    private final FileSystemPath cachePath;
    
    public JobCacheStatus(FileSystemPath cachePath, LockCacheStatus lockCacheStatus) {
        this.cachePath = cachePath;
        this.lockCacheStatus = lockCacheStatus;
    }
    
    public LockCacheStatus getLockCacheStatus() {
        return lockCacheStatus;
    }
    
    public FileSystemPath getCachePath() {
        return cachePath;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        
        JobCacheStatus that = (JobCacheStatus) obj;
        return this.cachePath.equals(that.cachePath) && this.lockCacheStatus.equals(that.lockCacheStatus);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(cachePath, lockCacheStatus);
    }
    
}
