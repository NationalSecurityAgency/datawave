package datawave.microservice.lock;

import java.util.concurrent.TimeUnit;

public interface Lock {
    String getName();
    
    void lock() throws Exception;
    
    void lockInterruptibly() throws InterruptedException;
    
    boolean tryLock();
    
    boolean tryLock(long time, TimeUnit unit) throws Exception;
    
    void unlock() throws Exception;
}
