package datawave.microservice.query.storage;

import java.util.concurrent.TimeUnit;

import datawave.microservice.cached.LockableCacheInspector;

/**
 * A lock object for a query status
 */
public class QueryStorageLockImpl implements QueryStorageLock {
    private final String cacheName;
    private final String storageKey;
    private final LockableCacheInspector cacheInspector;
    
    public QueryStorageLockImpl(String cacheName, String storageKey, LockableCacheInspector cacheInspector) {
        this.cacheName = cacheName;
        this.storageKey = storageKey;
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * Acquires the lock.
     */
    @Override
    public void lock() {
        cacheInspector.lock(cacheName, storageKey);
    }
    
    /**
     * Acquires the lock for the specified lease time.
     *
     * @param leaseTimeMillis
     *            The lease time in millis
     */
    @Override
    public void lock(long leaseTimeMillis) {
        cacheInspector.lock(cacheName, storageKey, leaseTimeMillis, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Acquires the lock for the specified query status.
     *
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     */
    @Override
    public boolean tryLock() {
        boolean locked = cacheInspector.tryLock(cacheName, storageKey);
        return locked;
    }
    
    /**
     * Determine if the lock is already acquired
     *
     * @return true if the lock is already acquired, false otherwise
     */
    @Override
    public boolean isLocked() {
        boolean locked = cacheInspector.isLocked(cacheName, storageKey);
        return locked;
    }
    
    /**
     * Acquires the lock within a specified amount of time.
     *
     * @param waitTimeMillis
     *            The wait time in millis
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     */
    @Override
    public boolean tryLock(long waitTimeMillis) throws InterruptedException {
        boolean locked = cacheInspector.tryLock(cacheName, storageKey, waitTimeMillis, TimeUnit.MILLISECONDS);
        return locked;
    }
    
    /**
     * Acquires the lock for the specified lease time.
     *
     * @param waitTimeMillis
     *            The wait time in millis
     * @param leaseTimeMillis
     *            Time to wait before automatically releasing the lock
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     */
    @Override
    public boolean tryLock(long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
        boolean locked = cacheInspector.tryLock(cacheName, storageKey, waitTimeMillis, TimeUnit.MILLISECONDS, leaseTimeMillis, TimeUnit.MILLISECONDS);
        return locked;
    }
    
    /**
     * Releases the lock for the specified query status
     */
    @Override
    public void unlock() {
        if (isLocked()) {
            cacheInspector.unlock(cacheName, storageKey);
        }
    }
    
    /**
     * Releases the lock for the specified query status regardless of the lock owner. It always successfully unlocks the key, never blocks, and returns
     * immediately.
     */
    @Override
    public void forceUnlock() {
        cacheInspector.forceUnlock(cacheName, storageKey);
    }
}
