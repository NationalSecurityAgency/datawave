package datawave.microservice.lock.distributed.hazelcast;

import com.hazelcast.cp.lock.FencedLock;
import datawave.microservice.lock.Lock;

import java.util.concurrent.TimeUnit;

public class HazelcastLock implements Lock {
    private final FencedLock lock;

    HazelcastLock(FencedLock lock) {
        this.lock = lock;
    }

    @Override
    public String getName() {
        return lock.getName();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }
}
