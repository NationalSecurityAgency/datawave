package datawave.ingest.util.cache.lease;

import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public interface JobCacheLockFactory extends AutoCloseable {
    ServiceLoader<JobCacheLockFactory> serviceLoader = ServiceLoader.load(JobCacheLockFactory.class);
    
    /**
     * Get the lock factory that is associated with the input mode.
     *
     * @param inputMode
     *            User input mode associated with a lock factory
     * @return A JobCacheLockFactory
     */
    static JobCacheLockFactory getLockFactory(String inputMode) {
        // @formatter:off
        Optional<JobCacheLockFactory> optLockFactory = StreamSupport
                .stream(serviceLoader.spliterator(), false)
                .filter(lockFactoryMode -> lockFactoryMode.getMode().name().equals(inputMode))
                .findFirst();
        // @formatter:on
        
        if (!optLockFactory.isPresent()) {
            throw new IllegalArgumentException(inputMode + " is not a valid mode. " + Arrays.toString(Mode.values()));
        }
        return optLockFactory.get();
    }
    
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
    
    /**
     * Get enum representing mode specified
     *
     * @return A mode to specify type of lock factory
     */
    JobCacheLockFactory.Mode getMode();
    
    /** Mode enum for specifying the method to find files to load */
    enum Mode {
        ZOOKEEPER, NO_OP
    }
    
}
