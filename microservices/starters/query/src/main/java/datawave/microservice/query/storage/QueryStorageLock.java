package datawave.microservice.query.storage;

public interface QueryStorageLock {
    
    /**
     * Acquires the lock.
     */
    void lock();
    
    /**
     * Acquires the lock for the specified lease time.
     *
     * @param leaseTimeMillis
     *            The lease time in millis
     */
    void lock(long leaseTimeMillis);
    
    /**
     * Acquires the lock for the specified query status.
     *
     * @return true if the lock was acquired, false if otherwise
     */
    boolean tryLock();
    
    /**
     * Determine if the lock is already acquired
     *
     * @return true if the lock is already acquired, false otherwise
     */
    boolean isLocked();
    
    /**
     * Acquires the lock within a specified amount of time.
     *
     * @param waitTimeMillis
     *            The wait time in millis
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @throws InterruptedException
     *             if interrupted
     */
    boolean tryLock(long waitTimeMillis) throws InterruptedException;
    
    /**
     * Acquires the lock for the specified lease time.
     *
     * @param waitTimeMillis
     *            The wait time in millis
     * @param leaseTimeMillis
     *            Time to wait before automatically releasing the lock
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @throws InterruptedException
     *             if interrupted
     */
    boolean tryLock(long waitTimeMillis, long leaseTimeMillis) throws InterruptedException;
    
    /**
     * Releases the lock for the specified query status
     */
    void unlock();
    
    /**
     * Releases the lock for the specified query status regardless of the lock owner. It always successfully unlocks the key, never blocks, and returns
     * immediately.
     */
    void forceUnlock();
}
