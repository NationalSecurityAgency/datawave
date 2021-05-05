package datawave.microservice.lock.distributed.zookeeper;

import datawave.microservice.lock.Lock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.TimeUnit;

public class ZookeeperLock implements Lock {
    private final String path;
    private final InterProcessMutex lock;
    
    ZookeeperLock(String path, InterProcessMutex lock) {
        this.path = path;
        this.lock = lock;
    }
    
    @Override
    public String getName() {
        return path;
    }
    
    @Override
    public void lock() throws Exception {
        lock.acquire();
    }
    
    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException("Unable to lockInterruptibly for ZookeeperLock");
    }
    
    @Override
    public boolean tryLock() {
        throw new UnsupportedOperationException("Unable to tryLock for ZookeeperLock");
    }
    
    @Override
    public boolean tryLock(long time, TimeUnit unit) throws Exception {
        return lock.acquire(time, unit);
    }
    
    @Override
    public void unlock() throws Exception {
        lock.release();
    }
}
