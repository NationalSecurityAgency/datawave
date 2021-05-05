package datawave.microservice.common.storage.lock;

import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;

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

public class ZooQueryLockManager implements QueryLockManager {
    
    // The curator client
    private CuratorFramework client;
    
    // The base zookeeper path for all of the locks
    private static final String BASE_PATH = "/QueryStorageCache/";
    
    // The set of query lock objects being used by this JVM
    private Map<UUID, QuerySemaphore> semaphores = Collections.synchronizedMap(new HashMap<>());
    
    public ZooQueryLockManager(String zookeeperConnectionString) {
        client = CuratorFrameworkFactory.builder().connectString(zookeeperConnectionString).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        client.start();
    }
    
    /**
     * This will setup a semaphore for a query with a specified count. This can be called multiple times for the same query if the number of available locks
     * needs to be adjusted over time. Specifying a count of 0 will effectively delete the semaphore.
     *
     * @param queryId
     *            The query id
     * @param count
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
     * @throws IOException
     *             If there was a lock system access failure
     */
    private boolean exists(UUID queryId) throws TaskLockException {
        // first try the quick test
        if (semaphores.containsKey(queryId)) {
            return true;
        }
        
        try {
            return (client.checkExists().forPath(getBasePath(queryId)) != null);
        } catch (Exception e) {
            throw new TaskLockException("Failed to examine zookeeper path for " + getBaseTaskLockPath(queryId), e);
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
     * Get the task ids for all locks that the zookeeper cluster knows about
     *
     * @param queryId
     * @return A set of task UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedLockedTasks(UUID queryId) throws IOException {
        try {
            List<String> paths = client.getChildren().forPath(getBaseTaskLockPath(queryId));
            return paths.stream().map(u -> UUID.fromString(u)).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine zookeeper path for " + getBaseTaskLockPath(queryId), e);
        }
    }
    
    /**
     * Get the query ids for all semaphores that the zookeeper cluster knows about
     *
     * @return The set of query UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedQueries() throws IOException {
        try {
            List<String> paths = client.getChildren().forPath(BASE_PATH);
            return paths.stream().map(u -> UUID.fromString(u)).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine zookeeper path for " + BASE_PATH, e);
        }
    }
    
    /**
     * Get a query lock that is expected to exist.
     * 
     * @param queryId
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
     * Get the base path for all locks, semaphores, etc for a query
     * 
     * @param queryId
     *            The queryId
     * @return the base path
     */
    private static String getBasePath(UUID queryId) {
        return BASE_PATH + queryId;
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
        private final SharedCount maxPermits;
        private final InterProcessSemaphoreV2 semaphore;
        private final Map<TaskKey,LeaseAndLock> leases = new HashMap<>();
        
        private class LeaseAndLock {
            final Lease lease;
            final InterProcessMutex lock;
            
            LeaseAndLock(Lease lease, InterProcessMutex lock) {
                this.lease = lease;
                this.lock = lock;
            }
            
            public void close() throws IOException {
                try {
                    lock.release();
                } catch (Exception e) {
                    throw new IOException("Unable to release lock", e);
                } finally {
                    lease.close();
                }
            }
        }
        
        QuerySemaphore(UUID queryId) throws TaskLockException {
            this.queryId = queryId;
            // first lets setup the shared count if needed
            maxPermits = getSemaphoreCount(queryId, DEFAULT_PERMITS);
            try {
                maxPermits.start();
            } catch (Exception e) {
                throw new TaskLockException("Unable to start zookeeper counter", e);
            }
            
            // now lets create the semaphore
            semaphore = getSemaphore(queryId, maxPermits);
        }
        
        QuerySemaphore(UUID queryId, int count) throws IOException {
            this(queryId);
            
            // set the number of permits
            updatePermits(count);
        }
        
        synchronized void updatePermits(int count) throws IOException {
            try {
                maxPermits.setCount(count);
            } catch (Exception e) {
                throw new IOException("Unable to set zookeeper counter", e);
            }
        }
        
        synchronized void close() throws IOException {
            try {
                for (LeaseAndLock lease : leases.values()) {
                    lease.close();
                }
            } finally {
                leases.clear();
                maxPermits.close();
            }
        }
        
        synchronized void delete() throws IOException {
            try {
                close();
            } finally {
                try {
                    client.delete().deletingChildrenIfNeeded().forPath(getBasePath(queryId));
                } catch (Exception e) {
                    throw new IOException("Failed to delete query lock for " + queryId, e);
                }
            }
        }
        
        synchronized boolean acquireLock(TaskKey task, long waitMs) throws InterruptedException, TaskLockException {
            if (leases.containsKey(task)) {
                throw new TaskLockException("Task already locked locally: " + task);
            }
            Lease lease = null;
            InterProcessMutex lock = null;
            boolean acquiredLock = false;
            try {
                // first get a semaphore lease
                lease = semaphore.acquire(waitMs, TimeUnit.MILLISECONDS);
                if (lease == null) {
                    return false;
                }
                
                // acquire the lock
                lock = getTaskLock(task);
                acquiredLock = lock.acquire(waitMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new TaskLockException("Unable to acquire a task lock", e);
            } finally {
                if (acquiredLock) {
                    leases.put(task, new LeaseAndLock(lease, lock));
                    return true;
                } else {
                    if (lease != null) {
                        try {
                            lease.close();
                        } catch (IOException e) {
                            throw new TaskLockException("Task is already locked: " + task + " AND failed to release lease", e);
                        }
                    }
                    throw new TaskLockException("Task is already locked: " + task);
                }
            }
        }
        
        synchronized void releaseLock(TaskKey task) throws TaskLockException {
            LeaseAndLock lease = leases.remove(task);
            if (lease == null) {
                throw new TaskLockException("Task not locked: " + task);
            }

            try {
                lease.close();
            } catch (IOException e) {
                throw new TaskLockException("Failed to close lease", e);
            }
        }
        
        synchronized boolean isLocked(TaskKey task) {
            return leases.containsKey(task);
        }
        
        synchronized Set<TaskKey> getLockedTasks() {
            return new HashSet<>(leases.keySet());
        }
        
        private SharedCount getSemaphoreCount(UUID queryId, int count) {
            return new SharedCount(client, getSemaphoreCountPath(queryId), count);
        }
        
        private InterProcessSemaphoreV2 getSemaphore(UUID queryId, SharedCount count) {
            return new InterProcessSemaphoreV2(client, getSemaphorePath(queryId), count);
        }
        
        private InterProcessMutex getTaskLock(TaskKey task) {
            return new InterProcessMutex(client, getTaskLockPath(task));
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
     * A Task lock handles getting a lock for a task.  A lock cannot be obtained until
     * there is a query semaphore permit available and then only if that task is not already
     * locked.
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
        private InterProcessMutex lock;

        public BasicLock(String id) {
            this.id = id;
            this.lock = new InterProcessMutex(client, getOtherLockPath(id));
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            try {
                return lock.acquire(time, unit);
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                throw new TaskLockException("Unable to acquire lock " + id, e);
            }
        }

        @Override
        public void unlock() {
            try {
                lock.release();
            } catch (Exception e) {
                throw new TaskLockException("Failed to unlock " + id, e);
            }
        }
    }



}
