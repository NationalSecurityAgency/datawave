package datawave.microservice.cached;

import java.util.concurrent.TimeUnit;

/**
 * Extends CacheInspector functionality to enable locking mechanisms, either native to the cache implementation or otherwise.
 */
public interface LockableCacheInspector extends CacheInspector {
    
    /**
     * Acquires the lock for the specified cache and key.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to lock
     */
    void lock(String cacheName, String key);
    
    /**
     * Acquires the lock for the specified cache and key for the specified lease time.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to lock
     * @param leaseTime
     *            time to wait before releasing the lock
     * @param leaseTimeUnit
     *            unit of time to specify lease time
     */
    void lock(String cacheName, String key, long leaseTime, TimeUnit leaseTimeUnit);
    
    /**
     * Tries to acquire the lock for the specified cache and key.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to lock
     */
    boolean tryLock(String cacheName, String key);
    
    /**
     * Tries to acquire the lock for the specified cache and key.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to lock
     * @param waitTime
     *            maximum time to wait for the lock
     * @param waitTimeUnit
     *            unit of time to specify wait time
     */
    boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit) throws InterruptedException;
    
    /**
     * Tries to acquire the lock for the specified cache and key for the specified lease time.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to lock
     * @param waitTime
     *            maximum time to wait for the lock
     * @param waitTimeUnit
     *            unit of time to specify wait time
     * @param leaseTime
     *            time to wait before releasing the lock
     * @param leaseTimeUnit
     *            unit of time to specify lease time
     */
    boolean tryLock(String cacheName, String key, long waitTime, TimeUnit waitTimeUnit, long leaseTime, TimeUnit leaseTimeUnit) throws InterruptedException;
    
    /**
     * Releases the lock for the specified cache and key.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to unlock
     */
    void unlock(String cacheName, String key);
    
    /**
     * Releases the lock for the specified cache and key regardless of the lock owner.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to unlock
     */
    void forceUnlock(String cacheName, String key);
    
    /**
     * Checks the lock for the specified cache and key.
     *
     * @param cacheName
     *            the name of the cache
     * @param key
     *            the key to unlock
     * @return If the lock is acquired then returns true, else returns false.
     */
    boolean isLocked(String cacheName, String key);
}
