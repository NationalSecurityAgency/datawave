package datawave.microservice.common.storage.lock;

import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class LocalQueryLockManager implements QueryLockManager {
    
    // The set of query semaphores which maintain the task lock states
    private Map<UUID,QuerySemaphore> semaphores = Collections.synchronizedMap(new HashMap<>());
    
    // The set of other arbitrary locks
    private Set<String> otherLocks = Collections.synchronizedSet(new HashSet<>());
    
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
    public void createSemaphore(UUID queryId, int count) {
        QuerySemaphore lock = semaphores.get(queryId);
        if (count > 0) {
            if (lock != null) {
                synchronized (lock) {
                    lock.maxPermits = count;
                }
            } else {
                semaphores.put(queryId, new QuerySemaphore(count));
            }
        } else {
            if (lock != null) {
                synchronized (lock) {
                    semaphores.remove(queryId);
                }
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
    public void closeSemaphore(UUID queryId) {
        QuerySemaphore lock = semaphores.get(queryId);
        if (lock != null) {
            synchronized (lock) {
                semaphores.remove(queryId);
            }
        }
    }
    
    @Override
    public Lock getLock(TaskKey task) {
        QuerySemaphore lock = semaphores.get(task.getQueryId());
        if (lock != null) {
            return new TaskLock(task);
        } else {
            throw new TaskLockException("No such query lock exists: " + task);
        }
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
    public boolean isLocked(TaskKey task) {
        QuerySemaphore lock = semaphores.get(task.getQueryId());
        if (lock != null) {
            synchronized (lock) {
                return lock.locks.contains(task);
            }
        }
        return false;
    }
    
    /**
     * Get the list of tasks that currently have locks
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     */
    @Override
    public Set<TaskKey> getLockedTasks(UUID queryId) {
        QuerySemaphore lock = semaphores.get(queryId);
        if (lock != null) {
            synchronized (lock) {
                return new HashSet<>(lock.locks);
            }
        }
        return Collections.emptySet();
    }
    
    /**
     * Get the list of queries that currently have semaphores
     */
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
     *
     * @return A set of task UUIDs
     */
    @Override
    public Set<UUID> getDistributedLockedTasks(UUID queryId) {
        return getLockedTasks(queryId).stream().map(c -> c.getTaskId()).collect(Collectors.toSet());
    }
    
    /**
     * Get the query ids for all semaphores that the cluster knows about
     *
     * @return The set of query UUIDs
     */
    @Override
    public Set<UUID> getDistributedQueries() {
        return getQueries();
    }
    
    /**
     * A QuerySemphore maintains a set of task permits for a query
     */
    private class QuerySemaphore {
        int maxPermits;
        Set<TaskKey> locks = new HashSet<>();
        
        public QuerySemaphore(int count) {
            maxPermits = count;
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
            long start = System.currentTimeMillis();
            QuerySemaphore lock = semaphores.get(task.getQueryId());
            if (lock != null) {
                while (true) {
                    synchronized (lock) {
                        if (semaphores.containsKey(task.getQueryId())) {
                            if (lock.locks.contains(task)) {
                                throw new TaskLockException("Task already locked: " + task);
                            }
                            if (lock.locks.size() < lock.maxPermits) {
                                lock.locks.add(task);
                                return true;
                            }
                        } else {
                            throw new TaskLockException("No such query lock exists: " + task);
                        }
                    }
                    if ((System.currentTimeMillis() - start) >= unit.toMillis(time)) {
                        return false;
                    } else {
                        Thread.sleep(1);
                    }
                }
            } else {
                throw new TaskLockException("No such query lock exists: " + task);
            }
        }
        
        @Override
        public void unlock() {
            QuerySemaphore lock = semaphores.get(task.getQueryId());
            if (lock != null) {
                synchronized (lock) {
                    if (!lock.locks.contains(task)) {
                        throw new TaskLockException("Task not locked: " + task);
                    }
                    lock.locks.remove(task);
                }
            }
        }
        
    }
    
    /**
     * A basic lock that is based on an arbitrary string lock id.
     */
    private class BasicLock extends GenericLock {
        private final String id;
        
        public BasicLock(String id) {
            this.id = id;
        }
        
        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            long start = System.currentTimeMillis();
            while (true) {
                synchronized (otherLocks) {
                    if (otherLocks.contains(id)) {
                        if ((System.currentTimeMillis() - start) >= unit.toMillis(time)) {
                            return false;
                        } else {
                            Thread.sleep(1);
                        }
                    } else {
                        otherLocks.add(id);
                        return true;
                    }
                }
            }
        }
        
        @Override
        public void unlock() {
            otherLocks.remove(id);
        }
    }
    
}
