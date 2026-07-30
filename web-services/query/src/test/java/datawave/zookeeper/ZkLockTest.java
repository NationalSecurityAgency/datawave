package datawave.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ZkLockTest {

    private TestingServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.close();
        }
    }

    /**
     * Verify that when the timeout is exceeded when attempting to acquire a lock that is already held, a {@link TimeoutException} is thrown.
     */
    @Test
    void testCallWithLockWhenTimeoutExceeded() throws Exception {
        try (CuratorFramework client = getClient()) {
            // Configure a timeout of 1 second.
            ZkLock lock = new ZkLock(client, "/locks", 1, TimeUnit.SECONDS);

            // Obtain a lock for "id" and hold it for 3 seconds.
            CompletableFuture.runAsync(() -> {
                try {
                    lock.callWithLock("id", () -> {
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Verify that we get a timeout exception because we cannot obtain the lock for 'id' within 1 second.
            assertThatThrownBy(() -> lock.callWithLock("id", () -> null)).isInstanceOf(TimeoutException.class)
                            .hasMessage("Failed to acquire lock for 'id' within 1 SECONDS");
        }
    }

    /**
     * Verify that we can obtain multiple locks for different lock IDs.
     */
    @Test
    void testCallWithLockDoesNotBlockCallWithDifferentId() throws Exception {
        try (CuratorFramework client = getClient()) {
            // Configure a timeout of 1 second.
            ZkLock lock = new ZkLock(client, "/locks", 1, TimeUnit.SECONDS);

            // Obtain a lock for "id" and hold it for 3 seconds.
            CompletableFuture.runAsync(() -> {
                try {
                    lock.callWithLock("id", () -> {
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            // Verify that we do not get a timeout exception since we are obtaining a lock for another id.
            assertThatNoException().isThrownBy(() -> lock.callWithLock("otherId", () -> null));
        }
    }

    /**
     * Verify that when a lock is successfully acquired within the timeout, the result of the callable is returned.
     */
    @Test
    void testCallWithLockWhenTimeoutNotExceeded() throws Exception {
        try (CuratorFramework client = getClient()) {
            ZkLock lock = new ZkLock(client, "/locks", 3, TimeUnit.SECONDS);

            SingleThreadGuard guard = new SingleThreadGuard();
            int count = lock.callWithLock("id", guard::incrementAndGet);

            assertThat(count).isEqualTo(1);
        }
    }

    /**
     * Verify that when contending threads are attempting to acquire locks for the same id, they are only allowed to execute the callable one at a time.
     */
    @Test
    void testContendingThreads() throws InterruptedException {
        try (CuratorFramework client = getClient()) {
            ZkLock lock = new ZkLock(client, "/locks", 3, TimeUnit.SECONDS);

            // Create a list of 10 random callables that will attempt to increment the count in the SingleThreadGuard via the ZLock.
            SingleThreadGuard guard = new SingleThreadGuard();
            List<RandomCallable> callables = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                callables.add(new RandomCallable(lock, guard));
            }

            // Create a thread pool of 10 threads.
            ExecutorService executor = Executors.newFixedThreadPool(10);

            // Verify that no exception is thrown by the lock.tryLock() in the SingleThreadGuard modified by any of the callables executing in the thread pool.
            assertThatNoException().isThrownBy(() -> executor.invokeAll(callables));

            // The final count value of the SingleThreadGuard should be equal to the total number of increments summed from all callables.
            int totalIncrements = callables.stream().map(RandomCallable::getTotalIncrements).reduce(0, Integer::sum);
            assertThat(totalIncrements).isEqualTo(guard.count);
            executor.shutdown();
        }
    }

    /**
     * Test class that will throw an exception if any threads try to obtain the lock while another thread is holding it.
     */
    private static class SingleThreadGuard {
        private int count = 0;
        private final ReentrantLock lock = new ReentrantLock();

        public int incrementAndGet() {
            // Immediately throw an exception if another thread is already holding the lock. This should never happen when access is guarded by the ZLock.
            if (!lock.tryLock()) {
                throw new IllegalStateException("Multiple threads are trying to access this instance.");
            }
            try {
                count++;
                return count;
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * A callable class that will attempt to invoke {@link ZkLock#callWithLock(String, Callable)} at random intervals between 50-75 ms, and then hold the lock
     * for 100 ms.
     */
    private static class RandomCallable implements Callable<Void> {
        private final ZkLock zLock;
        private final SingleThreadGuard guard;
        private final AtomicInteger totalIncrements = new AtomicInteger(0);

        public RandomCallable(ZkLock zkLock, SingleThreadGuard guard) {
            this.zLock = zkLock;
            this.guard = guard;
        }

        public int getTotalIncrements() {
            return totalIncrements.get();
        }

        @Override
        public Void call() {
            long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(5);
            while (System.currentTimeMillis() < endTime) {
                try {
                    zLock.callWithLock("id", () -> {
                        guard.incrementAndGet();
                        // Hold the lock for at least 100 ms before releasing it.
                        Thread.sleep(100);
                        return null;
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                totalIncrements.incrementAndGet();
                // Sleep a random delay of 50-75 ms to increase likelihood of attempting to obtain the lock while another thread is holding it.
                long randomDelay = ThreadLocalRandom.current().nextLong(50, 75);
                try {
                    Thread.sleep(randomDelay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return null;
        }
    }

    private CuratorFramework getClient() throws InterruptedException {
        // @formatter:off
        CuratorFramework client = CuratorFrameworkFactory.builder()
                        .connectString(server.getConnectString())
                        .sessionTimeoutMs(60000)
                        .connectionTimeoutMs(60000)
                        .retryPolicy(new RetryNTimes(10, 1000))
                        .build();
        // @formatter:on
        client.start();
        client.blockUntilConnected(5, TimeUnit.SECONDS);
        return client;
    }
}
