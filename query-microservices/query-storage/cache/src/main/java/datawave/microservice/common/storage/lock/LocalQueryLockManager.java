package datawave.microservice.common.storage.lock;

import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class LocalQueryLockManager implements QueryLockManager {
    
    private class QueryLock {
        int maxPermits;
        Set<TaskKey> locks = new HashSet<>();
        
        public QueryLock(int count) {
            maxPermits = count;
        }
    }
    
    private Map<UUID,QueryLock> semaphores = Collections.synchronizedMap(new HashMap<>());
    
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
        QueryLock lock = semaphores.get(queryId);
        if (count > 0) {
            if (lock != null) {
                synchronized (lock) {
                    lock.maxPermits = count;
                }
            } else {
                semaphores.put(queryId, new QueryLock(count));
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
        QueryLock lock = semaphores.get(queryId);
        if (lock != null) {
            synchronized (lock) {
                semaphores.remove(queryId);
            }
        }
    }

    @Override
    public Lock getLock(TaskKey task) {
        return null;
    }

    @Override
    public Lock getLock(UUID queryId) {
        return null;
    }

    @Override
    public Lock getLock(String lockId) {
        return null;
    }

    /**
     * Acquire a lock for a task. This will wait the specified waitMs for a semaphore slot to be available.
     *
     * @param task
     *            The task to lock
     * @param waitMs
     *            How long to wait for semaphore availability
     * @return true if able to lock the task
     * @throws TaskLockException
     *             if the task is already locked or the query semaphore does not exist
     */
    @Override
    public boolean acquireLock(TaskKey task, long waitMs) throws TaskLockException {
        long start = System.currentTimeMillis();
        QueryLock lock = semaphores.get(task.getQueryId());
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
                if ((System.currentTimeMillis() - start) >= waitMs) {
                    return false;
                } else {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        // we were interrupted, return immediately, no lock acquired
                        return false;
                    }
                }
            }
        } else {
            throw new TaskLockException("No such query lock exists: " + task);
        }
    }
    
    /**
     * Release the lock for a task. This will also decrement the semaphore allowing another task to be locked.
     *
     * @param task
     *            The task to unlock
     * @throws TaskLockException
     *             if the task is not locked.
     */
    @Override
    public void releaseLock(TaskKey task) throws TaskLockException {
        QueryLock lock = semaphores.get(task.getQueryId());
        if (lock != null) {
            synchronized (lock) {
                if (!lock.locks.contains(task)) {
                    throw new TaskLockException("Task not locked: " + task);
                }
                lock.locks.remove(task);
            }
        }
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
        QueryLock lock = semaphores.get(task.getQueryId());
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
        QueryLock lock = semaphores.get(queryId);
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
     * Get the task ids for all locks that the zookeeper cluster knows about
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
     * Get the query ids for all semaphores that the zookeeper cluster knows about
     *
     * @return The set of query UUIDs
     */
    @Override
    public Set<UUID> getDistributedQueries() {
        return getQueries();
    }
}
