package datawave.microservice.lock.local;

import datawave.microservice.lock.Lock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class LocalLock implements Lock {

    private final String name;
    private final ReentrantLock lock;

    LocalLock(String name, ReentrantLock lock) {
        this.name = name;
        this.lock = lock;
    }

    @Override
    public String getName() {
        return name;
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
