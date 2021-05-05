package datawave.microservice.lock.distributed.zookeeper;

import datawave.microservice.lock.Semaphore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class ZookeeperSemaphore implements Semaphore {
    private final String path;
    private final InterProcessSemaphoreV2 semaphore;
    private final LinkedList<Lease> leases = new LinkedList<>();
    private final CuratorFramework curatorFramework;
    
    ZookeeperSemaphore(String path, InterProcessSemaphoreV2 semaphore, CuratorFramework curatorFramework) {
        this.path = path;
        this.semaphore = semaphore;
        this.curatorFramework = curatorFramework;
        ;
    }
    
    @Override
    public String getName() {
        return path;
    }
    
    @Override
    public void acquire() throws Exception {
        synchronized (leases) {
            leases.push(semaphore.acquire());
        }
    }
    
    @Override
    public void acquire(int permits) throws Exception {
        synchronized (leases) {
            semaphore.acquire(permits).forEach(leases::push);
        }
    }
    
    @Override
    public int availablePermits() throws Exception {
        return curatorFramework.getChildren().forPath(path).size();
    }
    
    @Override
    public int drainPermits() throws Exception {
        throw new UnsupportedOperationException("Unable to drainPermits for ZookeeperSemaphore");
    }
    
    @Override
    public void release() {
        synchronized (leases) {
            if (leases.size() > 0) {
                semaphore.returnLease(leases.pop());
            }
        }
    }
    
    @Override
    public void release(int permits) {
        synchronized (leases) {
            while (permits-- > 0 && !leases.isEmpty()) {
                semaphore.returnLease(leases.pop());
            }
        }
    }
    
    @Override
    public boolean tryAcquire() {
        throw new UnsupportedOperationException("Unable to tryAcquire for ZookeeperSemaphore");
    }
    
    @Override
    public boolean tryAcquire(int permits) {
        throw new UnsupportedOperationException("Unable to tryAcquire for ZookeeperSemaphore");
    }
    
    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Unable to tryAcquire for ZookeeperSemaphore");
    }
    
    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("Unable to tryAcquire for ZookeeperSemaphore");
    }
}
