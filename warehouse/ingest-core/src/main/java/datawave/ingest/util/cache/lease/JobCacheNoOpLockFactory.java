package datawave.ingest.util.cache.lease;

import java.util.function.Predicate;

public class JobCacheNoOpLockFactory implements JobCacheLockFactory {
    
    @Override
    public boolean acquireLock(String id) {
        return true;
    }
    
    @Override
    public boolean acquireLock(String id, Predicate<String> cacheAvailablePredicate) {
        return true;
    }
    
    @Override
    public boolean releaseLock(String id) {
        return true;
    }
    
    @Override
    public Mode getMode() {
        return Mode.NO_OP;
    }
    
    @Override
    public void close() {}
    
    @Override
    public Predicate<String> getCacheAvailablePredicate() {
        return input -> true;
    }
    
    @Override
    public LockCacheStatus getCacheStatus(String id) {
        return null;
    }
    
}
