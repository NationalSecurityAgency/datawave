package datawave.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

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
     * Create and return a new {@link ZkLock} that uses the given client and root path when creating locks, and the given default timeout and time unit when
     * waiting for locks to be acquired.
     *
     * @param client
     *            the Zookeeper client
     * @param rootPath
     *            the root path
     * @param defaultTimeout
     *            the default timeout
     * @param defaultTimeUnit
     *            the time unit
     */
    public ZkLock(CuratorFramework client, String rootPath, int defaultTimeout, TimeUnit defaultTimeUnit) {
        this.defaultTimeout = defaultTimeout;
        this.defaultTimeUnit = defaultTimeUnit;
        this.rootPath = rootPath;
        this.client = client;
    }

    /**
     * The root path used when creating locks for a given lock ID.
     *
     * @return the root path
     */
    public String getRootPath() {
        return rootPath;
    }

    /**
     * Obtain a distributed lock from Zookeeper, and invoke and return the result of {@link Callable#call()} on the given callable while holding the lock. The
     * lock will automatically be released after {@link Callable#call()} is invoked. The default timeout and time unit configured for this {@link ZkLock} will
     * be used when attempting to acquire the lock.
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
        String lockPath = rootPath + "/" + lockId;
        InterProcessMutex lock = new InterProcessMutex(client, lockPath);
        boolean acquired = lock.acquire(timeout, timeUnit);
        if (!acquired) {
            throw new TimeoutException("Failed to acquire lock for '" + lockId + "' within " + timeout + " " + timeUnit);
        }
        try {
            return callable.call();
        } finally {
            lock.release();
        }
    }
}
