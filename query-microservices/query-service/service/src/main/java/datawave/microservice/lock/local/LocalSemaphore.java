package datawave.microservice.lock.local;

import datawave.microservice.lock.Semaphore;

import java.util.concurrent.TimeUnit;

public class LocalSemaphore implements Semaphore {

    private String name;
    private java.util.concurrent.Semaphore semaphore;

    LocalSemaphore(String name, java.util.concurrent.Semaphore semaphore) {
        this.name = name;
        this.semaphore = semaphore;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        semaphore.acquire(permits);
    }

    @Override
    public int availablePermits() {
        return semaphore.availablePermits();
    }

    @Override
    public int drainPermits() {
        return semaphore.drainPermits();
    }

    @Override
    public void release() {
        semaphore.release();
    }

    @Override
    public void release(int permits) {
        semaphore.release();
    }

    @Override
    public boolean tryAcquire() {
        return semaphore.tryAcquire();
    }

    @Override
    public boolean tryAcquire(int permits) {
        return semaphore.tryAcquire(permits);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return semaphore.tryAcquire(timeout, unit);
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        return semaphore.tryAcquire(permits, timeout, unit);
    }
}
