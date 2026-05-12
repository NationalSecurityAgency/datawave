package datawave.webservice.zookeeper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LockedZkClientDispatcher}.
 */
class LockedZkClientDispatcherTest {

    private TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify that when a {@link LockedZkClientDispatcher} created with an infinite maxElapsedAccessTime (0 or negative), the executor service is not created
     * when {@link LockedZkClientDispatcher#getLockedClient()} is called.
     */
    @Test
    void testDispatcherWithInfiniteMaxElapsedAccessTime() throws Exception {
        ExposedDispatcher dispatcher = new ExposedDispatcher(createClientFactory(), -1, TimeUnit.MILLISECONDS);
        // Verify the inner client and executor service are null before the first call to getLockedClient();
        assertNull(dispatcher.getClient());
        assertNull(dispatcher.getExecutor());

        LockedZkClientDispatcher.LockedClient lockedClient = dispatcher.getLockedClient();

        // Verify the lastClientAccess time was updated.
        assertNotEquals(0L, dispatcher.getLastClientAccess());

        // Verify the cleanup executor service is not created.
        assertNull(dispatcher.getExecutor());

        // Verify the client was initialized and accessible via the lockedClient.
        assertNotNull(dispatcher.getClient());
        assertSame(dispatcher.getClient(), lockedClient.getClient());

        // Verify the client lock is locked.
        assertTrue(dispatcher.getClientLock().isLocked());

        lockedClient.close();

        // Verify the client lock is unlocked after closing the locked client.
        assertFalse(dispatcher.getClientLock().isLocked());

        // Close the client.
        dispatcher.close();

        // Verify the client and executor are null.
        assertNull(dispatcher.getClient());
        assertNull(dispatcher.getExecutor());
    }

    /**
     * Verify that when a {@link LockedZkClientDispatcher} created with an finite maxElapsedAccessTime (0 or negative), the executor service is created when
     * {@link LockedZkClientDispatcher#getLockedClient()} is called.
     */
    @Test
    void testDispatcherWithFiniteMaxElapsedAccessTime() throws Exception {
        ExposedDispatcher dispatcher = new ExposedDispatcher(createClientFactory(), 120000, TimeUnit.MILLISECONDS);
        // Verify the inner client and executor service are null before the first call to getLockedClient();
        assertNull(dispatcher.getClient());
        assertNull(dispatcher.getExecutor());

        LockedZkClientDispatcher.LockedClient lockedClient = dispatcher.getLockedClient();

        // Verify the lastClientAccess time was updated.
        assertNotEquals(0L, dispatcher.getLastClientAccess());

        // Verify the cleanup executor service was created, and a task was added to it to handle cleanup.
        ScheduledThreadPoolExecutor executor = dispatcher.getExecutor();
        assertNotNull(executor);
        assertFalse(executor.getQueue().isEmpty());

        // Verify the client was initialized and accessible via the lockedClient.
        assertNotNull(dispatcher.getClient());
        assertSame(dispatcher.getClient(), lockedClient.getClient());

        // Verify the client lock is locked.
        assertTrue(dispatcher.getClientLock().isLocked());

        lockedClient.close();

        // Verify the client lock is unlocked after closing the locked client.
        assertFalse(dispatcher.getClientLock().isLocked());

        // Close the client.
        dispatcher.close();

        // Verify the client and executor are null.
        assertNull(dispatcher.getClient());
        assertNull(dispatcher.getExecutor());
    }

    /**
     * Verify that as long as the max elapsed access time is not met, the executor service will reschedule its cleanup task.
     */
    @Test
    void testCleanupTaskReschedulesSelfIfNotTimedOut() throws Exception {
        ExposedDispatcher dispatcher = new ExposedDispatcher(createClientFactory(), 500, TimeUnit.MILLISECONDS);

        // The task should get rescheduled at least 4 times.
        for (int i = 0; i < 10; i++) {
            try (LockedZkClientDispatcher.LockedClient ignored = dispatcher.getLockedClient()) {
                Thread.sleep(250);
            }
        }

        // Get the total completed
        long taskCompletedCount = dispatcher.getExecutor().getCompletedTaskCount();

        // Verify the cleanup task was executed between 4-6 times.
        assertTrue(taskCompletedCount >= 4 && taskCompletedCount < 6);
        dispatcher.close();
    }

    /**
     * Verify that when the max elapsed time is met, the client is cleaned up by the executor service.
     */
    @Test
    void testClientIsCleanedUpWhenMaxElapsedAccessTimeIsReached() throws Exception {
        ExposedDispatcher dispatcher = new ExposedDispatcher(createClientFactory(), 500, TimeUnit.MILLISECONDS);
        // Verify the client and executor service are initialized after the first call to get the client.
        try (LockedZkClientDispatcher.LockedClient ignored = dispatcher.getLockedClient()) {
            assertNotNull(dispatcher.getClient());
            assertNotNull(dispatcher.getExecutor());
        }

        try {
            // Attempt to wait until the client is null. This may take a little bit due to the call to CuratorFramework.close().
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> dispatcher.getClient() == null);
        } catch (Exception e) {
            fail("Expected client to be cleaned up within 5 seconds");
        }

        // Verify the executor is not null and does not have any tasks in the queue.
        ScheduledThreadPoolExecutor executor = dispatcher.getExecutor();
        assertNotNull(executor);
        assertTrue(executor.getQueue().isEmpty());

        dispatcher.close();
    }

    /**
     * Verify there are no issues creating a new client after an old one has been cleaned up.
     */
    @Test
    void testGetClientAfterCleanUp() throws Exception {
        ExposedDispatcher dispatcher = new ExposedDispatcher(createClientFactory(), 500, TimeUnit.MILLISECONDS);
        // Verify the client and executor service are initialized after the first call to get the client.
        try (LockedZkClientDispatcher.LockedClient ignored = dispatcher.getLockedClient()) {
            assertNotNull(dispatcher.getClient());
            assertNotNull(dispatcher.getExecutor());
        }

        try {
            // Attempt to wait until the client is null. This may take a little bit due to the call to CuratorFramework.close().
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> dispatcher.getClient() == null);
        } catch (Exception e) {
            fail("Expected client to be cleaned up within 5 seconds");
        }

        // Verify the executor is not null and does not have any tasks in the queue.
        ScheduledThreadPoolExecutor executor = dispatcher.getExecutor();
        assertNotNull(executor);
        assertTrue(executor.getQueue().isEmpty());

        // Get a fresh client.
        try (LockedZkClientDispatcher.LockedClient ignored = dispatcher.getLockedClient()) {
            // Verify the client is not null.
            assertNotNull(dispatcher.getClient());

            // Verify the executor service is not null, and has a task in the queue to watch for the next elapsed time.
            executor = dispatcher.getExecutor();
            assertNotNull(executor);
            assertFalse(executor.getQueue().isEmpty());
        }

        dispatcher.close();
    }

    private CuratorFrameworkFactory.Builder createClientFactory() {
        return CuratorFrameworkFactory.builder().connectString(server.getConnectString()).sessionTimeoutMs(60000).connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000));
    }

    /**
     * An implementation of {@link LockedZkClientDispatcher} that exposes its inner members for testing.
     */
    public static class ExposedDispatcher extends LockedZkClientDispatcher {

        public ExposedDispatcher(CuratorFrameworkFactory.Builder clientFactory, long maxElapsedAccessTime, TimeUnit timeUnit) {
            super(clientFactory, maxElapsedAccessTime, maxElapsedAccessTime, timeUnit);
        }

        public CuratorFramework getClient() {
            return client;
        }

        public ReentrantLock getClientLock() {
            return clientLock;
        }

        public ScheduledThreadPoolExecutor getExecutor() {
            return executor;
        }

        public long getLastClientAccess() {
            return lastClientAccess;
        }
    }
}
