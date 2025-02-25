package datawave.microservice.querymetric;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;

public class MergeLockLifecycleListener implements LifecycleListener, HazelcastInstanceAware {
    
    private static Logger log = LoggerFactory.getLogger(MergeLockLifecycleListener.class);
    public WriteLockRunnable writeLockRunnable;
    private ReentrantReadWriteLock clusterLock;
    private Future writeLockRunnableFuture;
    private HazelcastInstance instance;
    private String localMemberUuid;
    private AtomicBoolean allowReadLock = new AtomicBoolean(false);
    private AtomicBoolean shuttingDown = new AtomicBoolean(false);
    
    public MergeLockLifecycleListener() {
        this.clusterLock = new ReentrantReadWriteLock(true);
        this.writeLockRunnable = new WriteLockRunnable(this.clusterLock);
        this.writeLockRunnableFuture = Executors.newSingleThreadExecutor().submit(this.writeLockRunnable);
    }
    
    @PreDestroy
    public void preDestroy() {
        makeServiceUnavailable();
        try {
            this.writeLockRunnableFuture.get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            this.writeLockRunnableFuture.cancel(true);
        }
    }
    
    public void setAllowReadLock(boolean allowReadLock) {
        this.allowReadLock.set(allowReadLock);
    }
    
    public boolean isAllowedReadLock() {
        return this.allowReadLock.get();
    }
    
    public boolean isShuttingDown() {
        return shuttingDown.get();
    }
    
    @Override
    public void setHazelcastInstance(HazelcastInstance instance) {
        this.instance = instance;
    }
    
    private String getLocalMemberUuid() {
        try {
            if (this.instance != null) {
                this.localMemberUuid = this.instance.getCluster().getLocalMember().getUuid().toString();
            }
        } catch (HazelcastInstanceNotActiveException e) {
            // this.instance.getCluster() will throw an exception during shutdown
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return (this.localMemberUuid == null) ? "" : this.localMemberUuid;
    }
    
    private void makeServiceAvailable() {
        setAllowReadLock(true);
    }
    
    private void makeServiceUnavailable() {
        this.setAllowReadLock(false);
        this.shuttingDown.set(true);
    }
    
    // A writeLock around STARTING and STARTED is taken care of in HazelcastMetricCacheConfiguration.hazelcastInstance to
    // ensure that the lastWrittenQueryMetricCache is set into the MapStore before the instance is active and the writeLock is released
    // multiple log lines are used to keep log/lock sequencing consistent (lock, log, <do stuff>, log, unlock) for paired events
    @Override
    public void stateChanged(LifecycleEvent event) {
        switch (event.getState()) {
            case MERGING:
                // lock for a maximum time so that we don't lock forever
                this.writeLockRunnable.lock(event.getState(), 5, TimeUnit.MINUTES);
                log.info(event + " [" + getLocalMemberUuid() + "]");
                break;
            case SHUTTING_DOWN:
                // lock for a maximum time so that we don't lock forever
                this.writeLockRunnable.lock(event.getState(), 60, TimeUnit.SECONDS);
                makeServiceUnavailable();
                log.info(event + " [" + getLocalMemberUuid() + "]");
                break;
            case MERGED:
            case SHUTDOWN:
                log.info(event + " [" + getLocalMemberUuid() + "]");
                this.writeLockRunnable.unlock(event.getState());
                break;
            case MERGE_FAILED:
                log.info(event + " [" + getLocalMemberUuid() + "]");
                makeServiceUnavailable();
                this.writeLockRunnable.unlock(event.getState());
                QueryMetricService.shutdown();
                break;
            default:
                log.info(event + " [" + getLocalMemberUuid() + "]");
        }
    }
    
    public void lock() {
        while (!this.allowReadLock.get()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("locking for read");
        }
        clusterLock.readLock().lock();
        if (log.isTraceEnabled()) {
            log.trace("locked for read");
        }
    }
    
    public void unlock() {
        if (log.isTraceEnabled()) {
            log.trace("unlocking for read");
        }
        clusterLock.readLock().unlock();
        if (log.isTraceEnabled()) {
            log.trace("unlocked for read");
        }
    }
    
    public class WriteLockRunnable implements Runnable {
        
        private ReentrantReadWriteLock clusterLock;
        private AtomicBoolean requestLock = new AtomicBoolean();
        private AtomicBoolean requestUnlock = new AtomicBoolean();
        private long scheduledUnlockTime = Long.MAX_VALUE;
        
        public WriteLockRunnable(ReentrantReadWriteLock clusterLock) {
            this.clusterLock = clusterLock;
        }
        
        @Override
        public void run() {
            while (!isShuttingDown()) {
                if (this.requestLock.get() == true) {
                    this.clusterLock.writeLock().lock();
                    // allow the lock() thread to continue
                    this.requestLock.set(false);
                    try {
                        // wait for unlock() to be called or a scheduled unlock
                        while (requestUnlock.get() == false && System.currentTimeMillis() < this.scheduledUnlockTime && !shuttingDown.get()) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                        }
                    } finally {
                        this.clusterLock.writeLock().unlock();
                        // allow the unlock() thread to continue
                        this.requestUnlock.set(false);
                    }
                } else {
                    try {
                        Thread.sleep(100);
                        // If a write lock timed out and the clusterLock is no longer locked when requestUnlock
                        // happens, we should reset requestUnlock to false and allow the requesting thread to continue
                        if (this.requestUnlock.get() == true && !this.clusterLock.isWriteLocked()) {
                            this.requestUnlock.set(false);
                        }
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }
        
        public void lock(LifecycleEvent.LifecycleState state) {
            lock(state, -1, TimeUnit.MILLISECONDS);
        }
        
        public void lock(LifecycleEvent.LifecycleState state, long maxDuration, TimeUnit timeUnit) {
            log.info("locking for write [" + state + "]");
            // prompt run() method to lock the writeLock
            if (this.requestLock.compareAndSet(false, true)) {
                if (maxDuration >= 0) {
                    this.scheduledUnlockTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(maxDuration, timeUnit);
                } else {
                    this.scheduledUnlockTime = Long.MAX_VALUE;
                }
                // wait until run() method locks the writeLock and sets requestLock to false
                while (requestLock.get() == true && !isShuttingDown()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
            log.info("locked for write [" + state + "]");
        }
        
        public void unlock(LifecycleEvent.LifecycleState state) {
            log.info("unlocking for write [" + state + "]");
            // cancel any scheduled unlock
            this.scheduledUnlockTime = Long.MAX_VALUE;
            // prompt run() method to unlock the writeLock
            if (this.requestUnlock.compareAndSet(false, true)) {
                // wait until run() method unlocks the writeLock and sets requestUnlock to false
                while (requestUnlock.get() == true && !isShuttingDown()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
            log.info("unlocked for write [" + state + "]");
            if (state.equals(LifecycleEvent.LifecycleState.STARTED)) {
                makeServiceAvailable();
            }
        }
    }
}
