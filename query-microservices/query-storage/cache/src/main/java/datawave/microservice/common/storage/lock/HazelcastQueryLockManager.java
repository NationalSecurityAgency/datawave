package datawave.microservice.common.storage.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.cp.lock.FencedLock;
import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class HazelcastQueryLockManager implements QueryLockManager {
    
    // The hazelcast instance
    private final HazelcastInstance instance;
    
    // The hazelcast client
    private final CPSubsystem client;
    
    // The base path for all of the locks
    private static final String BASE_PATH = "/QueryStorageCache/";
    
    // The set of query lock objects being used by this JVM
    private Map<UUID,QuerySemaphore> semaphores = Collections.synchronizedMap(new HashMap<>());
    
    public HazelcastQueryLockManager(HazelcastInstance hazelcastInstance) {
        this.instance = hazelcastInstance;
        this.client = hazelcastInstance.getCPSubsystem();
    }
    
    /**
     * This will setup a semaphore for a query with a specified count. This can be called multiple times for the same query if the number of available locks
     * needs to be adjusted over time. Specifying a count of 0 will effectively delete the semaphore.
     *
     * @param queryId
     *            The query id
     * @param count
     *            The max permits
     */
    @Override
    public void createSemaphore(UUID queryId, int count) throws IOException {
        if (count == 0) {
            QuerySemaphore lock = semaphores.remove(queryId);
            if (lock != null) {
                lock.delete();
            }
        } else {
            QuerySemaphore lock = semaphores.get(queryId);
            if (lock != null) {
                lock.updatePermits(count);
            } else {
                semaphores.put(queryId, new QuerySemaphore(queryId, count));
            }
        }
    }
    
    /**
     * This will drop any cached data for this queryid, leaving the query locks in the underlying cluster. Any local task locks will be released.
     *
     * @param queryId
     *            The query id
     */
    @Override
    public void closeSemaphore(UUID queryId) throws IOException {
        QuerySemaphore lock = semaphores.remove(queryId);
        if (lock != null) {
            lock.close();
        }
    }
    
    @Override
    public Lock getLock(TaskKey task) {
        return new TaskLock(task);
    }
    
    @Override
    public Lock getLock(UUID queryId) {
        return getLock(queryId.toString());
    }
    
    @Override
    public Lock getLock(String lockId) {
        return new BasicLock(lockId);
    }
    
    /**
     * Determine if a task is locked.
     *
     * @param task
     *            The task
     * @return True if locked
     */
    @Override
    public boolean isLocked(TaskKey task) throws IOException {
        return getQueryLock(task.getQueryId()).isLocked(task);
    }
    
    /**
     * Determine if the specified query exists in the underlying cluster
     *
     * @param queryId
     *            The query id
     * @return true if it exists
     * @throws TaskLockException
     *             If there was a lock system access failure
     */
    private boolean exists(UUID queryId) throws TaskLockException {
        // first try the quick test
        if (semaphores.containsKey(queryId)) {
            return true;
        }
        
        try {
            IAtomicLong count = client.getAtomicLong(getSemaphoreCountPath(queryId));
            return (count.get() != 0);
        } catch (Exception e) {
            throw new TaskLockException("Failed to examine hazelcast path for " + getSemaphoreCountPath(queryId), e);
        }
    }
    
    /**
     * Get the list of tasks that currently have locks (in this JVM)
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     */
    @Override
    public Set<TaskKey> getLockedTasks(UUID queryId) throws IOException {
        return getQueryLock(queryId).getLockedTasks();
    }
    
    /**
     * Get the list of queries that currently have semaphores (in this JVM)
     */
    @Override
    public Set<UUID> getQueries() {
        synchronized (semaphores) {
            return new HashSet<>(semaphores.keySet());
        }
    }
    
    /**
     * Get the task ids for all locks that the cluster knows about
     *
     * @param queryId
     *            The query id
     * @return A set of task UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedLockedTasks(UUID queryId) throws IOException {
        final String basePath = getBaseTaskLockPath(queryId);
        try {
            return instance.getDistributedObjects().stream().map(o -> o.getName()).filter(o -> o.startsWith(basePath)).map(u -> UUID.fromString(u))
                            .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine hazelcast path for " + basePath, e);
        }
    }
    
    /**
     * Get the query ids for all semaphores that the hazelcast cluster knows about
     *
     * @return The set of query UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedQueries() throws IOException {
        final String basePath = getBaseQueryPath();
        try {
            return instance.getDistributedObjects().stream().map(o -> o.getName()).filter(o -> o.startsWith(basePath)).map(u -> UUID.fromString(u))
                            .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine hazelcast path for " + basePath, e);
        }
    }
    
    /**
     * Get a query lock that is expected to exist.
     * 
     * @param queryId
     *            The query id
     * @return the query lock
     * @throws IllegalStateException
     *             if the query lock has not already been created
     */
    private QuerySemaphore getQueryLock(UUID queryId) throws TaskLockException {
        QuerySemaphore lock = semaphores.get(queryId);
        if (lock == null) {
            if (exists(queryId)) {
                lock = new QuerySemaphore(queryId);
                semaphores.put(queryId, lock);
            } else {
                throw new IllegalStateException("Query semaphore does not exist for " + queryId);
            }
        }
        return lock;
    }
    
    /**
     * Get the base path for all queries
     * 
     * @return the base query path
     */
    private static String getBaseQueryPath() {
        return BASE_PATH + "queries/";
    }
    
    /**
     * Get the base path for all locks, semaphores, etc for a specific query
     * 
     * @param queryId
     *            The queryId
     * @return the base path
     */
    private static String getBasePath(UUID queryId) {
        return getBaseQueryPath() + queryId;
    }
    
    /**
     * Get the semaphore count path for a query which holds the max number of permits.
     * 
     * @param queryId
     *            The queryId
     * @return the semaphore count path
     */
    private static String getSemaphoreCountPath(UUID queryId) {
        return getBasePath(queryId) + "/semaphoreCount";
    }
    
    /**
     * Get the semaphore path for a query
     * 
     * @param queryId
     *            The queryid
     * @return the semaphore path
     */
    private static String getSemaphorePath(UUID queryId) {
        return getBasePath(queryId) + "/semaphore";
    }
    
    /**
     * Get the base task lock path for a query
     * 
     * @param queryId
     * @return the base task lock path
     */
    private static String getBaseTaskLockPath(UUID queryId) {
        return getBasePath(queryId) + "/locks";
    }
    
    /**
     * Get the task lock path for a task
     * 
     * @param task
     *            The task key
     * @return the task lock path
     */
    private static String getTaskLockPath(TaskKey task) {
        return getBaseTaskLockPath(task.getQueryId()) + '/' + task.getTaskId();
    }
    
    /**
     * Get the path for some other lock
     * 
     * @param id
     * @return the lock path
     */
    private static String getOtherLockPath(String id) {
        return BASE_PATH + "OtherLocks/" + id;
    }
    
    /**
     * The query lock class which handles creating and deleting query semaphores and task locks
     */
    private class QuerySemaphore {
        private static final int DEFAULT_PERMITS = 1;
        private final UUID queryId;
        private final IAtomicLong maxPermits;
        private final ISemaphore semaphore;
        private final Map<TaskKey,FencedLock> locks = new HashMap<>();
        
        QuerySemaphore(UUID queryId) throws TaskLockException {
            this.queryId = queryId;
            // first lets setup the shared count if needed
            maxPermits = getSemaphoreCount(queryId);
            
            // now lets create the semaphore
            semaphore = getSemaphore(queryId, maxPermits, DEFAULT_PERMITS);
        }
        
        QuerySemaphore(UUID queryId, int count) throws IOException {
            this(queryId);
            
            // set the number of permits
            updatePermits(count);
        }
        
        synchronized void updatePermits(int count) throws IOException {
            try {
                int current = (int) (maxPermits.get());
                maxPermits.set(count);
                if (current > count) {
                    semaphore.reducePermits(current - count);
                } else if (current < count) {
                    semaphore.increasePermits(count - current);
                }
            } catch (Exception e) {
                throw new IOException("Unable to set hazelcast counter", e);
            }
        }
        
        synchronized void close() throws IOException {
            try {
                for (FencedLock lock : locks.values()) {
                    this.semaphore.release();
                    lock.unlock();
                }
            } finally {
                locks.clear();
            }
        }
        
        synchronized void delete() throws IOException {
            try {
                close();
            } finally {
                try {
                    maxPermits.destroy();
                    semaphore.destroy();
                } catch (Exception e) {
                    throw new IOException("Failed to delete query lock for " + queryId, e);
                }
            }
        }
        
        synchronized boolean acquireLock(TaskKey task, long waitMs) throws InterruptedException, TaskLockException {
            if (locks.containsKey(task)) {
                throw new TaskLockException("Task already locked locally: " + task);
            }
            FencedLock lock = null;
            boolean acquiredLock = false;
            try {
                // first get a semaphore lease
                if (!semaphore.tryAcquire(waitMs, TimeUnit.MILLISECONDS)) {
                    return false;
                }
                
                // acquire the lock
                lock = getTaskLock(task);
                acquiredLock = lock.tryLock(waitMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new TaskLockException("Unable to acquire a task lock", e);
            } finally {
                if (acquiredLock) {
                    locks.put(task, lock);
                    return true;
                } else {
                    semaphore.release();
                    throw new TaskLockException("Task is already locked: " + task);
                }
            }
        }
        
        synchronized void releaseLock(TaskKey task) throws TaskLockException {
            FencedLock lock = locks.remove(task);
            if (lock == null) {
                throw new TaskLockException("Task not locked: " + task);
            }
            
            try {
                this.semaphore.release();
                lock.unlock();
            } catch (Exception e) {
                throw new TaskLockException("Failed to close lease", e);
            }
        }
        
        synchronized boolean isLocked(TaskKey task) {
            return locks.containsKey(task);
        }
        
        synchronized Set<TaskKey> getLockedTasks() {
            return new HashSet<>(locks.keySet());
        }
        
        private IAtomicLong getSemaphoreCount(UUID queryId) {
            return client.getAtomicLong(getSemaphoreCountPath(queryId));
        }
        
        private ISemaphore getSemaphore(UUID queryId, IAtomicLong maxPermits, int seedValue) {
            ISemaphore semaphore = client.getSemaphore(getSemaphorePath(queryId));
            if (maxPermits.get() == 0) {
                maxPermits.set(seedValue);
                semaphore.init(seedValue);
            }
            return semaphore;
        }
        
        private FencedLock getTaskLock(TaskKey task) {
            return client.getLock(getTaskLockPath(task));
        }
    }
    
    /**
     * A generic lock that implements most of the Lock methods
     */
    private abstract class GenericLock implements Lock {
        @Override
        public void lock() {
            while (true) {
                try {
                    tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
                    return;
                } catch (InterruptedException ie) {
                    // try again
                }
            }
        }
        
        @Override
        public void lockInterruptibly() throws InterruptedException {
            tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
        
        @Override
        public boolean tryLock() {
            try {
                return tryLock(0, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                return false;
            }
        }
        
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Conditions not supported");
        }
    }
    
    /**
     * A Task lock handles getting a lock for a task. A lock cannot be obtained until there is a query semaphore permit available and then only if that task is
     * not already locked.
     */
    private class TaskLock extends GenericLock {
        private final TaskKey task;
        
        public TaskLock(TaskKey task) {
            this.task = task;
        }
        
        @Override
        public boolean tryLock(long time, TimeUnit unit) throws TaskLockException, InterruptedException {
            return getQueryLock(task.getQueryId()).acquireLock(task, unit.toMillis(time));
        }
        
        @Override
        public void unlock() {
            getQueryLock(task.getQueryId()).releaseLock(task);
        }
        
    }
    
    /**
     * A basic lock that is based on an arbitrary string lock id.
     */
    private class BasicLock extends GenericLock {
        private final String id;
        private FencedLock lock;
        
        public BasicLock(String id) {
            this.id = id;
            this.lock = client.getLock(getOtherLockPath(id));
        }
        
        @Override
        public boolean tryLock(long time, TimeUnit unit) {
            try {
                return lock.tryLock(time, unit);
            } catch (Exception e) {
                throw new TaskLockException("Unable to acquire lock " + id, e);
            }
        }
        
        @Override
        public void unlock() {
            try {
                lock.unlock();
            } catch (Exception e) {
                throw new TaskLockException("Failed to unlock " + id, e);
            }
        }
    }
    
}
