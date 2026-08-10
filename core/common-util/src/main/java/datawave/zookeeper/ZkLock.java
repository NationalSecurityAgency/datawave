package datawave.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class provides a convenient interface for configuring the creation of {@link InterProcessMutex} locks that will be obtained and used to execute callable
 * operations within the context of a held lock.
 * <p>
 * <b>Reentrancy:</b> {@link #callWithLock} is fully reentrant <i>per {@link ZkLock} instance</i>. Important notes:
 * <ul>
 * <li>A single {@link InterProcessMutex} is cached and shared per lock path, rather than created fresh on every call, which allows Curator's built-in
 * per-thread reentrant tracking to work as intended. A thread that already holds a given lock ID can call {@code callWithLock} again for that same ID (directly
 * or via a nested call several frames down) and it will succeed immediately without contending against itself.</li>
 * <li>Other threads calling with the same lock ID still properly contend for the lock via Zookeeper, as do calls made through a different {@link ZkLock}
 * instance (even one pointed at the same root path and client). The reentrant fast path only applies to calls made through the same {@link ZkLock} object.</li>
 * <li>As with Curator's {@link InterProcessMutex}, all reentrant acquisitions of a given lock ID must be released by the same thread that acquired them. This
 * is guaranteed automatically here since each {@code callWithLock} invocation performs exactly one acquire and one matching release, synchronously, on the
 * calling thread.</li>
 * </ul>
 * <p>
 * <b>Node deletion:</b> when node deletion after release is enabled (either via the constructor default or the per-call override), the lock's Zookeeper node is
 * deleted after the <i>outermost</i> release for that lock path completes (i.e., once no invocation, on any thread, is still using the cached lock for that
 * path) regardless of whether the supplied {@link Callable} completed normally or threw an exception. Deletion is still skipped if another participant
 * (including one in a different process) is found to be waiting on the lock.
 */
public class ZkLock {

    private static final Logger log = LoggerFactory.getLogger(ZkLock.class);

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
     * Cache of shared, reference-counted {@link InterProcessMutex} instances, keyed by lock path. Reusing a single mutex instance per path (rather than
     * creating a new one per call) is what allows Curator's per-thread reentrant tracking to recognize nested acquisitions by the same thread. An entry is
     * created on the first use of a lock path and removed once its reference count returns to 0, i.e., once no {@code callWithLock} invocations anywhere
     * (including nested ones) is still using that path.
     */
    private final ConcurrentHashMap<String,LockEntry> locks = new ConcurrentHashMap<>();

    /**
     * A cached {@link InterProcessMutex} for a given lock path, along with a count of how many in-flight {@code callWithLock} invocations (across all threads,
     * including reentrant, nested ones) are currently relying on it.
     */
    private static final class LockEntry {

        private final InterProcessMutex mutex;
        private final AtomicInteger refCount = new AtomicInteger();

        public LockEntry(InterProcessMutex mutex) {
            this.mutex = mutex;
        }
    }

    /**
     * Create and return a new {@link ZkLock} that uses the given client and root path when creating locks, and the given default timeout and time unit when
     * waiting for locks to be acquired. The provided client must be started before any additional calls to this {@link ZkLock}.
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
        Preconditions.checkArgument(defaultTimeout > 0, "defaultTimeout must be > 0");
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
     * @throws IllegalStateException
     *             if the calling thread already holds this lock (unsupported reentrant acquisition)
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
     * @throws IllegalStateException
     *             if the calling thread already holds this lock (unsupported reentrant acquisition)
     * @throws Exception
     *             if an error occurs while attempting to obtain the lock
     */
    public <T> T callWithLock(String lockId, int timeout, TimeUnit timeUnit, Callable<T> callable) throws Exception {
        return callWithLock(lockId, timeout, timeUnit, this.deleteLocksAfterRelease, callable);
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
     * @param deleteLocksAfterRelease
     *            whether the lock path {@code /<lockId>} should be deleted after release if no other threads are waiting to acquire it; overrides the
     *            instance-level default for this call only
     * @param callable
     *            the callable to invoke
     * @return the result of invoking {@link Callable#call()} on the callout
     * @param <T>
     *            the type returned by {@link Callable#call()}
     * @throws TimeoutException
     *             if the lock fails to be acquired within the timeout
     * @throws IllegalStateException
     *             if the calling thread already holds this lock (unsupported reentrant acquisition)
     * @throws Exception
     *             if an error occurs while attempting to obtain the lock
     */
    public <T> T callWithLock(String lockId, int timeout, TimeUnit timeUnit, boolean deleteLocksAfterRelease, Callable<T> callable) throws Exception {
        String lockPath = getLockPath(lockId);

        // Atomically get-or-create the shared entry for the lock path, and register this invocation's use of it.
        LockEntry entry = locks.compute(lockPath, (path, existing) -> {
            LockEntry e = existing != null ? existing : new LockEntry(new InterProcessMutex(client, lockPath));
            e.refCount.incrementAndGet();
            return e;
        });

        boolean acquired = false;
        try {
            try {
                // If the calling thread already holds this mutex (directly or via an outer callWithLock invocation for the same lock ID) Curator recognizes it
                // via its own internal per-thread tracking and this returns immediately without contending against itself.
                acquired = entry.mutex.acquire(timeout, timeUnit);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            if (!acquired) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to acquire lock for {} within timeout of {} {}. {} other participants waiting on lock.", lockPath, timeout, timeUnit,
                                    entry.mutex.getParticipantNodes().size());
                }
                throw new TimeoutException("Failed to acquire lock for " + lockPath + " within " + timeout + " " + timeUnit);
            }
            return callable.call();
        } finally {
            // This balances the acquire() above.
            if (acquired) {
                entry.mutex.release();
            }

            // Deregister this invocation's use of the entry. If no other invocation (any thread, including nested ones) is still using this lock path, remove
            // it from the cache.
            boolean stilInUse = locks.compute(lockPath, (path, existing) -> {
                if (existing == null) {
                    // Should not happen: we are the ones holding a reference count on this entry.
                    log.warn("Cached lock is null where it should have been impossible.");
                    return null;
                }
                return existing.refCount.decrementAndGet() > 0 ? existing : null;
            }) != null;

            // If the lock was acquired, is no longer in use, and we are to delete locks after release, attempt to do so.
            if (acquired && !stilInUse && deleteLocksAfterRelease) {
                try {
                    client.delete().forPath(lockPath);
                } catch (KeeperException.NoNodeException e) {
                    // Safe to ignore. Another participant (e.g., in a different process) queued up and created a child node.
                    if (log.isDebugEnabled()) {
                        log.debug("Not deleting lock node {} after release; another participant is still waiting on it.", lockPath);
                    }
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
        Preconditions.checkArgument(lockId != null && !lockId.isBlank(), "lockId must not be null or blank");
        String normalizedId = ZkUtils.normalizePath(lockId);
        return rootPath != null ? rootPath + normalizedId : normalizedId;
    }
}
