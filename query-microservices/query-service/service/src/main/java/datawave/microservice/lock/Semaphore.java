package datawave.microservice.lock;

import java.util.concurrent.TimeUnit;

public interface Semaphore {
    String getName();
    
    void acquire() throws Exception;
    
    void acquire(int permits) throws Exception;
    
    int availablePermits() throws Exception;
    
    int drainPermits() throws Exception;
    
    void release();
    
    void release(int permits);
    
    boolean tryAcquire();
    
    boolean tryAcquire(int permits);
    
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;
    
    boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException;
}
