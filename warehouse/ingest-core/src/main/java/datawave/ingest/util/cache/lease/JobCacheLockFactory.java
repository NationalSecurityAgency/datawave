package datawave.ingest.util.cache.lease;

import org.apache.hadoop.conf.Configuration;

import java.util.function.Predicate;

public interface JobCacheLockFactory extends AutoCloseable {
    
    /**
     * Acquire a lock based on the id.
     *
     * @param id
     *            An id that represents a resource to lock
     * @return True if a lock was acquired.
     */
    boolean acquireLock(String id);
    
    /**
     * Acquire a lock based on the id and the availability of the cache
     *
     * @param id
     *            An id that represents a resource to lock.
     * @param cacheAvailablePredicate
     *            A predicate function to determine if the cache is available.
     * @return True if a lock was acquired.
     */
    boolean acquireLock(String id, Predicate<String> cacheAvailablePredicate);
    
    /**
     * Will close all resources used by the locking factory
     */
    void close();
    
    /**
     * Predicate to determine if the cache is available for acquiring locks
     *
     * @return A predicate function that takes a string as input
     */
    Predicate<String> getCacheAvailablePredicate();
    
    /**
     * Get cache status for the given id
     * 
     * @param id
     *            An cache id
     *
     * @return An object representing the cache lock attributes.
     */
    LockCacheStatus getCacheStatus(String id);
    
    /**
     * Get cache status for the given id
     * 
     * @param conf
     *            The configuration to extract property values
     */
    default void init(Configuration conf) {}
    
    /**
     * Release lock for id if possible
     *
     * @param id
     *            An id that represents a resource to lock
     * @return True if the lock was released.
     */
    boolean releaseLock(String id);
}
