package datawave.webservice.zookeeper;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * A class that provides the ability to access a maintained {@link CuratorFramework} client that is guarded by a lock from access by multiple threads at the
 * same time. Access to the underlying client is provided via {@link LockedZkClientDispatcher#getLockedClient()}, and is intended to be used within the context
 * of a try-with-resources statement. For example:
 *
 * <pre>
 * LockedZkClientDispatcher dispatcher = new LockedZkClientDispatcher(clientFactory, clientCleanupInterval, maxElapsedAccessTime, timeUnit);
 * // Obtain guarded access to the client.
 * try (LockedZkClientDispatcher.LockedClient lockedClient = dispatcher.getLockedClient()) {
 *      CuratorFramework client = lockedClient.getClient();
 *      // Do things with the client.
 * }
 * // The lock will automatically be released after the try statement. Note: you should never call {@link CuratorFramework#close()} on the provided client.
 * Clean up of the client will be handled by the dispatcher.
 * </pre>
 *
 * If {@link LockedZkClientDispatcher#getLockedClient()} is not used with a try-with-resources statement, care must be taken to ensure
 * {@link LockedClient#close()} is called when you are finished with the client to release the lock. The underlying client will be cleaned up and resources
 * released if the {@link LockedZkClientDispatcher} is created with a non-zero, positive clientCleanupInterval, and the time since
 * {@link LockedZkClientDispatcher#getLockedClient()} meets or exceeds the max elapsed time.
 */
public class LockedZkClientDispatcher implements AutoCloseable {

    private static final Logger log = Logger.getLogger(LockedZkClientDispatcher.class);

    /**
     * The {@link CuratorFramework} factory.
     */
    protected final CuratorFrameworkFactory.Builder clientFactory;

    /**
     * The interval in milliseconds between checks for client access timeouts.
     */
    protected final long clientCleanupInterval;

    /**
     * The max time in milliseconds that can elapse since the last client access before cleanup will trigger on the next cleanup task.
     */
    protected final long maxElapsedAccessTime;

    /**
     * The lock that guards access to the client.
     */
    protected final ReentrantLock clientLock = new ReentrantLock();

    /**
     * The underlying client.
     */
    protected CuratorFramework client;

    /**
     * The thread pool that will check if the client should be cleaned up.
     */
    protected ScheduledThreadPoolExecutor executor;

    /**
     * The system time in milliseconds that {@link #initClient()} was last called.
     */
    protected long lastClientAccess = 0L;

    /**
     * Create and return a new {@link LockedZkClientDispatcher} that will not periodically clean up the internal zookeeper client.
     *
     * @param clientFactory
     *            the client factory
     */
    public LockedZkClientDispatcher(CuratorFrameworkFactory.Builder clientFactory) {
        this(clientFactory, -1, -1, null);
    }

    /**
     * Create and return a new {@link LockedZkClientDispatcher} with the given cleanup interval configuration. No cleanup of the client will occur if
     * clientCleanupInterval is 0 or less.
     *
     * @param clientFactory
     *            the client factory
     * @param clientCleanupInterval
     *            the interval at which the dispatcher will periodically clean up and close the client if maxElapsedAccessTime has elapsed since the last client
     *            access. A value of 0 or less will result in no client cleanup.
     * @param maxElapsedAccessTime
     *            the max time that may elapse between client accesses before cleanup of the client will be allowed. If the value is negative and a non-zero
     *            cleanup interval is provided, the maxElapsedAccessTime will default to 0.
     * @param timeUnit
     *            the time unit for both clientCleanupInterval and maxElapsedAccessTime
     */
    public LockedZkClientDispatcher(CuratorFrameworkFactory.Builder clientFactory, long clientCleanupInterval, long maxElapsedAccessTime, TimeUnit timeUnit) {
        Preconditions.checkNotNull(clientFactory, "clientFactory must not be null");

        this.clientFactory = clientFactory;

        if (clientCleanupInterval > 0) {
            Preconditions.checkNotNull(timeUnit, "timeUnit must not be null");
            // If the maxElapsedAccessTime is negative, default to 0.
            this.maxElapsedAccessTime = maxElapsedAccessTime > 0 ? timeUnit.toMillis(clientCleanupInterval) : 0;
            this.clientCleanupInterval = timeUnit.toMillis(clientCleanupInterval);
        } else {
            this.clientCleanupInterval = -1L;
            this.maxElapsedAccessTime = -1L;
        }
    }

    /**
     * Returns a {@link LockedClient} that has locked access to the underlying {@link CuratorFramework} of this {@link LockedZkClientDispatcher}. The underlying
     * client will be non-null and started. This method is intended to be used with a try-with-resources statement. If not used in that manner, you MUST call
     * {@link LockedClient#close()} to release the client lock once you are done with it.
     *
     * @return the new locked client
     */
    public LockedClient getLockedClient() {
        // Lock access to the client.
        clientLock.lock();
        // Ensure the client is initialized.
        initClient();
        // Return a locked client.
        return new LockedClient(client, clientLock);
    }

    /**
     * Initialize the underlying client if necessary, and set {@link #lastClientAccess} to the current system time.
     */
    private void initClient() {
        if (client == null) {
            clientLock.lock();
            try {
                // Create the client and start it.
                client = clientFactory.build();
                client.start();

                // If we have a non-zero cleanup interval, create the cleanup task.
                if (clientCleanupInterval > 0) {
                    createCleanupTask();
                }
            } finally {
                clientLock.unlock();
            }
        }
        // Update the last-accessed time if we are periodically cleaning up the client.
        if (clientCleanupInterval > 0) {
            lastClientAccess = System.currentTimeMillis();
        }
    }

    /**
     * Initialize the executor service if necessary, and add tasks that will check if we've reached the client timeout at the specified cleanup task intervals.
     */
    private void createCleanupTask() {
        if (executor == null) {
            executor = new ScheduledThreadPoolExecutor(1);
        }

        Runnable task = new Runnable() {
            @Override
            public void run() {
                // If the max elapsed timeout has been reached, clean up the client.
                if (System.currentTimeMillis() - lastClientAccess >= maxElapsedAccessTime) {
                    cleanupClient();
                } else {
                    // Otherwise, schedule another task to check again after the designated interval.
                    executor.schedule(this, clientCleanupInterval, TimeUnit.MILLISECONDS);
                }
            }
        };

        // Schedule the task.
        executor.schedule(task, clientCleanupInterval, TimeUnit.MILLISECONDS);
    }

    /**
     * Clean up the client.
     */
    private void cleanupClient() {
        if (client != null) {
            clientLock.lock();
            try {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        log.warn("Failed to close client", e);
                    } finally {
                        client = null;
                    }
                }
            } finally {
                clientLock.unlock();
            }
        }
    }

    /**
     * Clean up the client and the executor service.
     */
    private void cleanupClientAndExecutor() {
        clientLock.lock();
        try {
            if (executor != null) {
                try {
                    executor.shutdown();
                } catch (Exception e) {
                    log.warn("Failed to shutdown executor", e);
                }
                executor = null;
            }
            cleanupClient();
        } finally {
            clientLock.unlock();
        }
    }

    /**
     * Release the underlying resources held by this {@link LockedZkClientDispatcher}. The underlying {@link CuratorFramework} and executor service will be
     * stopped and nullified.
     *
     * @throws Exception
     *             if an error occurs during cleanup.
     */
    @Override
    public void close() throws Exception {
        cleanupClientAndExecutor();
    }

    /**
     * An {@link AutoCloseable} that holds a reference to the client provided by an instance of {@link LockedZkClientDispatcher}, and the client's associated
     * lock. The lock will be unlocked when {@link #close()} is called.
     */
    public static class LockedClient implements AutoCloseable {

        private final CuratorFramework client;
        private final Lock lock;

        private LockedClient(CuratorFramework client, Lock lock) {
            this.client = client;
            this.lock = lock;
        }

        /**
         * Return the guarded {@link CuratorFramework} client.
         *
         * @return the client
         */
        public CuratorFramework getClient() {
            return client;
        }

        /**
         * Release the client lock.
         *
         * @throws Exception
         *             if an error occurs
         */
        @Override
        public void close() throws Exception {
            lock.unlock();
        }
    }
}
