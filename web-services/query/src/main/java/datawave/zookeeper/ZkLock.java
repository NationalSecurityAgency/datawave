package datawave.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;

import com.google.common.base.Preconditions;

/**
 * This class provides a convenient interface for configuring the creation of {@link InterProcessMutex} locks that will be obtained and used to execute callable
 * operations within the context of a held lock.
 */
public class ZkLock {

    /**
     * The Zookeeper client.
     */
    private final CuratorFramework client;

    /**
     * The root path used when creating locks.
     */
    private final String rootPath;

    /**
     * The default timeout when acquiring locks.
     */
    private final int defaultTimeout;

    /**
     * The default timeout time unit.
     */
    private final TimeUnit defaultTimeUnit;

    /**
     * Whether the lock path {@code /<lockId>} created via the calls to {@link #callWithLock(String, Callable)} and
     * {@link #callWithLock(String, int, TimeUnit, Callable)} should be deleted if no other threads are waiting to acquire the lock for the lock ID.
     */
    private final boolean deleteLocksAfterRelease;

    /**
     * Create and return a new {@link ZkLock} that uses the given client and root path when creating locks, and the given default timeout and time unit when
     * waiting for locks to be acquired.
     *
     * @param client
     *            the Zookeeper client
     * @param rootPath
     *            the root path, which will be ignored if null or blank
     * @param defaultTimeout
     *            the default timeout
     * @param defaultTimeUnit
     *            the time unit
     * @param deleteLocksAfterRelease
     *            whether the lock paths created via {@link #callWithLock(String, Callable)} and {@link #callWithLock(String, int, TimeUnit, Callable)} should
     *            be deleted after the lock is released if no other threads are waiting to acquire the lock
     */
    public ZkLock(CuratorFramework client, String rootPath, int defaultTimeout, TimeUnit defaultTimeUnit, boolean deleteLocksAfterRelease) {
        Preconditions.checkNotNull(client, "client must not be null");
        Preconditions.checkArgument(defaultTimeout >= 0, "defaultTimeout must be >= 0");
        Preconditions.checkNotNull(defaultTimeUnit, "defaultTimeUnit must not be null");
        this.defaultTimeout = defaultTimeout;
        this.defaultTimeUnit = defaultTimeUnit;
        this.rootPath = ZkUtils.normalizePath(rootPath);
        this.client = client;
        this.deleteLocksAfterRelease = deleteLocksAfterRelease;
    }

    /**
     * Obtain a distributed lock from Zookeeper under the path {@code <rootPath>/<lockId>}, and invoke and return the result of {@link Callable#call()} on the
     * given callable while holding the lock. The lock will automatically be released after {@link Callable#call()} is invoked. The default timeout and time
     * unit configured for this {@link ZkLock} will be used when attempting to acquire the lock.
     *
     * @param lockId
     *            the lock to obtain
     * @param callable
     *            the callable to invoke
     * @return the result of invoking {@link Callable#call()} on the callout
     * @param <T>
     *            the type returned by {@link Callable#call()}
     * @throws TimeoutException
     *             if the lock fails to be acquired within the timeout
     * @throws Exception
     *             if an error occurs while attempting to obtain the lock
     */
    public <T> T callWithLock(String lockId, Callable<T> callable) throws Exception {
        return callWithLock(lockId, this.defaultTimeout, this.defaultTimeUnit, callable);
    }

    /**
     * Obtain a distributed lock from Zookeeper, and invoke and return the result of {@link Callable#call()} on the given callable while holding the lock. The
     * lock will automatically be released after {@link Callable#call()} is invoked.
     *
     * @param lockId
     *            the lock to obtain
     * @param timeout
     *            the timeout for acquiring the lock
     * @param timeUnit
     *            the time unit for the timeout
     * @param callable
     *            the callable to invoke
     * @return the result of invoking {@link Callable#call()} on the callout
     * @param <T>
     *            the type returned by {@link Callable#call()}
     * @throws TimeoutException
     *             if the lock fails to be acquired within the timeout
     * @throws Exception
     *             if an error occurs while attempting to obtain the lock
     */
    public <T> T callWithLock(String lockId, int timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception {
        String lockPath = getLockPath(lockId);
        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        boolean acquired = lock.acquire(timeout, timeUnit);
        if (!acquired) {
            throw new TimeoutException("Failed to acquire lock for " + lockPath + " within " + timeout + " " + timeUnit);
        }
        try {
            return callable.call();
        } finally {
            lock.release();
            if (deleteLocksAfterRelease) {
                try {
                    // Only succeeds of no other threads/processes are waiting.
                    client.delete().forPath(lockPath);
                } catch (KeeperException.NotEmptyException ignored) {
                    // Safe to ignore. Another thread queued up and created a child node.
                }
            }
        }
    }

    /**
     * Return a path for the given lock ID with the root path prefixed.
     *
     * @param lockId
     *            the lock ID
     * @return the lock path
     */
    private String getLockPath(String lockId) {
        String normalizedId = ZkUtils.normalizePath(lockId);
        if (normalizedId == null) {
            throw new IllegalArgumentException("Unable to normalize lock ID '" + lockId + "' to a valid Zookeeper path");
        }
        return rootPath != null ? rootPath + normalizedId : normalizedId;
    }
}
