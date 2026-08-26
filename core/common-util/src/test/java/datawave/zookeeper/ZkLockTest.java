package datawave.zookeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ZkLock}.
 */
class ZkLockTest {

    private static TestingServer testingServer;
    private static CuratorFramework client;

    @BeforeAll
    static void startServer() throws Exception {
        testingServer = new TestingServer();
        client = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        client.start();
        client.blockUntilConnected(10, TimeUnit.SECONDS);
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (client != null) {
            client.close();
        }
        if (testingServer != null) {
            testingServer.close();
        }
    }

    @AfterEach
    void cleanUp() throws Exception {
        // Wipe everything created by the previous test so tests don't leak state into one another.
        if (client.checkExists().forPath("/") != null) {
            for (String child : client.getChildren().forPath("/")) {
                if (!child.equals("zookeeper")) {
                    client.delete().deletingChildrenIfNeeded().forPath("/" + child);
                }
            }
        }
    }

    /**
     * Tests for {@link ZkLock#ZkLock(CuratorFramework, String, int, TimeUnit, boolean)}.
     */
    @Nested
    class ConstructorTests {

        /**
         * Verify the client cannot be null.
         */
        @Test
        void nullClientThrowsNullPointerException() {
            assertThatThrownBy(() -> new ZkLock(null, "/locks", 5, TimeUnit.SECONDS, false)).isInstanceOf(NullPointerException.class)
                            .hasMessageContaining("client");
        }

        /**
         * Verify the timeout cannot be 0.
         */
        @Test
        void zeroTimeoutThrowsIllegalArgumentException() {
            assertThatThrownBy(() -> new ZkLock(client, "/locks", 0, TimeUnit.SECONDS, false)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("defaultTimeout");
        }

        /**
         * Verify the timeout cannot be negative.
         */
        @Test
        void negativeTimeoutThrowsIllegalArgumentException() {
            assertThatThrownBy(() -> new ZkLock(client, "/locks", -1, TimeUnit.SECONDS, false)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("defaultTimeout");
        }

        /**
         * Verify the time unit cannot be null.
         */
        @Test
        void nullTimeUnitThrowsNullPointerException() {
            assertThatThrownBy(() -> new ZkLock(client, "/locks", 5, null, false)).isInstanceOf(NullPointerException.class)
                            .hasMessageContaining("defaultTimeUnit");
        }

        /**
         * Verify a blank root path is treated as no root.
         */
        @Test
        void blankRootPathIsTreatedAsNoRoot() throws Exception {
            ZkLock lock = new ZkLock(client, "   ", 5, TimeUnit.SECONDS, false);
            AtomicInteger ran = new AtomicInteger();
            lock.callWithLock("myLock", () -> {
                ran.incrementAndGet();
                return null;
            });
            assertThat(ran.get()).isEqualTo(1);
            assertThat(client.checkExists().forPath("/myLock")).isNotNull();
        }

        /**
         * Verify a null root path is treated as no root.
         */
        @Test
        void nullRootPathIsTreatedAsNoRoot() throws Exception {
            ZkLock lock = new ZkLock(client, null, 5, TimeUnit.SECONDS, false);
            lock.callWithLock("anotherLock", () -> null);
            assertThat(client.checkExists().forPath("/anotherLock")).isNotNull();
        }
    }

    /**
     * Tests that verify that locks are created in Zookeeper under the correct paths.
     */
    @Nested
    class LockPathTests {

        /**
         * Verify that the lock node is created under the expected path.
         */
        @Test
        void lockNodeCreatedUnderRootPath() throws Exception {
            ZkLock lock = new ZkLock(client, "/app/locks", 5, TimeUnit.SECONDS, false);
            CountDownLatch holding = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                executor.submit(() -> {
                    try {
                        lock.callWithLock("orderProcessing", () -> {
                            holding.countDown();
                            release.await();
                            return null;
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();
                await().atMost(5, TimeUnit.SECONDS).until(() -> client.checkExists().forPath("/app/locks/orderProcessing") != null);
            } finally {
                release.countDown();
                executor.shutdown();
            }
        }

        /**
         * Verify that a lock ID without a leading slash is normalized.
         */
        @Test
        void lockIdWithoutLeadingSlashIsNormalized() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            lock.callWithLock("noSlashHere", () -> null);
            assertThat(client.checkExists().forPath("/locks/noSlashHere")).isNotNull();
        }

        /**
         * Verify that a blank lock ID results in an exception.
         */
        @Test
        void blankLockIdThrowsIllegalArgumentException() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertThatThrownBy(() -> lock.callWithLock("   ", () -> null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("lockId must not be null or blank");
        }

        /**
         * Verify that a null lock ID results in an exception.
         */
        @Test
        void nullLockIdThrowsIllegalArgumentException() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertThatThrownBy(() -> lock.callWithLock(null, () -> null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("lockId must not be null or blank");
        }

        /**
         * Verify that a lock ID that resolves to an empty path results in an exception.
         */
        @Test
        void lockIdNormalizedToEmptyPathThrowsIllegalArgumentException() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertThatThrownBy(() -> lock.callWithLock("/", () -> null)).isInstanceOf(IllegalArgumentException.class)
                            .hasMessageContaining("lockId '/' normalized to an empty path");
        }

        /**
         * Verify that multiple consecutive slashes are collapsed.
         */
        @Test
        void trailingSlashInRootPathIsCollapsedToASingleSlash() throws Exception {
            ZkLock lock = new ZkLock(client, "root///", 5, TimeUnit.SECONDS, false);
            lock.callWithLock("//x//a//", () -> null);
            assertThat(client.checkExists().forPath("/root/x/a")).isNotNull();
        }
    }

    /**
     * Tests for verifying basic functionality.
     */
    @Nested
    class BasicFunctionalityTests {

        /**
         * Verify that the result of the callable is returned.
         */
        @Test
        void callableResultIsReturned() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            String result = lock.callWithLock("basic", () -> "hello");
            assertThat(result).isEqualTo("hello");
        }

        /**
         * Verify that the lock is released after a successful call.
         */
        @Test
        void lockIsReleasedAfterSuccessfulCall() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
                lock.callWithLock("release1", () -> null);
                // If the first call failed to release, this would block until the 5s default timeout.
                lock.callWithLock("release1", () -> null);
            });
        }

        /**
         * Verify that the lock is released even if the callable throws an exception.
         */
        @Test
        void lockIsReleasedAfterCallableThrowsUncheckedException() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertThatThrownBy(() -> lock.callWithLock("release2", () -> {
                throw new IllegalStateException("boom");
            })).isInstanceOf(IllegalStateException.class).hasMessage("boom");

            assertTimeoutPreemptively(Duration.ofSeconds(3), () -> lock.callWithLock("release2", () -> null));
        }

        /**
         * Verify that the lock is released even if the callable throws a checked exception.
         */
        @Test
        void lockIsReleasedAfterCallableThrowsCheckedException() {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            assertThatThrownBy(() -> lock.callWithLock("release3", () -> {
                throw new java.io.IOException("checked boom");
            })).isInstanceOf(java.io.IOException.class).hasMessage("checked boom");

            assertTimeoutPreemptively(Duration.ofSeconds(3), () -> lock.callWithLock("release3", () -> null));
        }
    }

    /**
     * Tests for verifying timeout behavior.
     */
    @Nested
    class TimeoutTests {

        /**
         * Verify that an exception is thrown if the call times out while waiting to acquire the lock.
         */
        @Test
        void timeoutExceptionThrownWhenLockUnavailable() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            CountDownLatch holding = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                executor.submit(() -> {
                    try {
                        lock.callWithLock("contested", () -> {
                            holding.countDown();
                            release.await();
                            return null;
                        });
                    } catch (Exception ignored) {
                        // expected on shutdown
                    }
                });
                assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();

                // Make a second call that should time out.
                long start = System.currentTimeMillis();
                // @formatter:off
                assertThatThrownBy(() -> lock.callWithLock("contested", 500, TimeUnit.MILLISECONDS, () -> "never"))
                                .isInstanceOf(TimeoutException.class)
                                .hasMessageContaining("contested");
                // @formatter:on

                // Verify that the timeout exception was thrown after the timeout elapsed.
                long elapsedMs = System.currentTimeMillis() - start;
                assertThat(elapsedMs).isGreaterThanOrEqualTo(50);
            } finally {
                release.countDown();
                executor.shutdown();
            }
        }

        /**
         * Verify that a second caller can obtain a lock for the same lock path after the lock is released by the first holder.
         */
        @Test
        void lockAcquiredSuccessfullyWithinTimeoutOnceReleased() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            CountDownLatch holding = new CountDownLatch(1);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            // Let the first holder obtain and hold the lock for 300 ms.
            try {
                executor.submit(() -> {
                    try {
                        lock.callWithLock("willRelease", () -> {
                            holding.countDown();
                            Thread.sleep(300);
                            return null;
                        });
                    } catch (Exception ignored) {}
                });
                assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();

                // First holder will release the lock after ~300ms. A 3 sec timeout should comfortably succeed.
                String result = lock.callWithLock("willRelease", 3, TimeUnit.SECONDS, () -> "acquired");

                // Verify that the second holder obtained the lock and returned its callable result.
                assertThat(result).isEqualTo("acquired");
            } finally {
                executor.shutdown();
            }
        }
    }

    /**
     * Tests for verifying the deletion of locks if configured.
     */
    @Nested
    class DeleteAfterReleaseTests {

        /**
         * Verify that when locks are to be deleted, the node does not exist in Zookeeper after the lock is released and there are no other processes trying to
         * obtain the lock.
         */
        @Test
        void nodeDeletedWhenNoWaitersAndFlagTrue() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, true);
            lock.callWithLock("deleteMe", () -> null);
            await().atMost(3, TimeUnit.SECONDS).until(() -> client.checkExists().forPath("/locks/deleteMe") == null);
        }

        /**
         * Verify that when locks are not to be deleted, the node still exists in Zookeeper after the lock is released and there are no other processes trying
         * to obtain the lock.
         */
        @Test
        void nodeRetainedWhenFlagFalse() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            lock.callWithLock("keepMe", () -> null);
            assertThat(client.checkExists().forPath("/locks/keepMe")).isNotNull();
        }

        /**
         * Verify that when locks are to be deleted, the node is not deleted from Zookeeper when the lock is released by an initial holder, and there are other
         * processes waiting to obtain the lock.
         */
        @Test
        void nodeNotEmptyExceptionIsSwallowedWhenAnotherThreadIsWaiting() throws Exception {
            ZkLock lockA = new ZkLock(client, "/locks", 10, TimeUnit.SECONDS, true);
            ZkLock lockB = new ZkLock(client, "/locks", 10, TimeUnit.SECONDS, true);
            CountDownLatch aHolding = new CountDownLatch(1);
            CountDownLatch aRelease = new CountDownLatch(1);
            CountDownLatch bAcquired = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            // Let the first holder A obtain and hold the lock via lockA.
            try {
                Future<?> futureA = executor.submit(() -> {
                    try {
                        lockA.callWithLock("shared", () -> {
                            aHolding.countDown();
                            aRelease.await();
                            return null;
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                assertThat(aHolding.await(5, TimeUnit.SECONDS)).isTrue();

                // Create a future B that will queue up to obtain the lock via lockB.
                Future<?> futureB = executor.submit(() -> {
                    try {
                        lockB.callWithLock("shared", () -> {
                            bAcquired.countDown();
                            return null;
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // Wait for B to queue up as a second child under the lock path.
                await().atMost(3, TimeUnit.SECONDS).until(() -> client.getChildren().forPath("/locks/shared").size() >= 2);

                aRelease.countDown();
                futureA.get(5, TimeUnit.SECONDS);

                // A's release-then-delete must not have thrown, and must not have destroyed B's chance to acquire.
                assertThat(bAcquired.await(5, TimeUnit.SECONDS)).isTrue();
                futureB.get(5, TimeUnit.SECONDS);

                // Now that both are done, the node should finally be cleaned up.
                await().atMost(3, TimeUnit.SECONDS).until(() -> client.checkExists().forPath("/locks/shared") == null);
            } finally {
                executor.shutdown();
            }
        }
    }

    /**
     * Concurrency tests.
     */
    @Nested
    class ConcurrencyTests {

        /**
         * Verify that even under high concurrency, acquiring the locks
         */
        @Test
        void mutualExclusionHoldsUnderHighConcurrency() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 30, TimeUnit.SECONDS, false);
            int threadCount = 50;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicInteger counter = new AtomicInteger(0);
            AtomicInteger currentConcurrent = new AtomicInteger(0);
            AtomicInteger maxConcurrent = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            try {
                // Submit tasks that will acquire the lock and update relevant counters.
                for (int i = 0; i < threadCount; i++) {
                    futures.add(executor.submit(() -> {
                        startLatch.await();
                        return lock.callWithLock("counter", () -> {
                            int concurrentNow = currentConcurrent.incrementAndGet();
                            // Use a read-sleep-write pattern to widen the race window so that any failure of mutual exclusion is likely to be observed as a
                            // lost update.
                            maxConcurrent.updateAndGet(m -> Math.max(m, concurrentNow));
                            int before = counter.get();
                            Thread.sleep(5);
                            counter.set(before + 1);
                            currentConcurrent.decrementAndGet();
                            return null;
                        });
                    }));
                }

                // Wait for the tasks to complete.
                startLatch.countDown();
                for (Future<?> f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }

                // Verify that all threads managed to obtain the lock at some point, but only one thread held the lock at any point in time.
                assertThat(counter.get()).isEqualTo(threadCount);
                assertThat(maxConcurrent.get()).isEqualTo(1);
            } finally {
                executor.shutdown();
            }
        }

        /**
         * Verify that locks on different lock IDs do not block each other.
         */
        @Test
        void locksOnDifferentIdsDoNotBlockEachOther() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 30, TimeUnit.SECONDS, false);
            int threadCount = 10;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch allEntered = new CountDownLatch(threadCount);
            CountDownLatch release = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            try {
                // Submit tasks that will obtain locks for different lock IDs.
                for (int i = 0; i < threadCount; i++) {
                    int id = i;
                    futures.add(executor.submit(() -> lock.callWithLock("lock-" + id, () -> {
                        allEntered.countDown();
                        release.await();
                        return null;
                    })));
                }

                // If different lock ids serialized each other, this would time out.
                assertThat(allEntered.await(5, TimeUnit.SECONDS)).isTrue();
                release.countDown();
                for (Future<?> f : futures) {
                    f.get(5, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdown();
            }
        }

        /**
         * Verify that when there are many queued threads waiting for a lock (with reasonable timeouts), they all eventually acquire the lock, and cleanup
         * occurs.
         */
        @Test
        void manyQueuedThreadsAllEventuallyAcquireAndCleanupOccurs() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 60, TimeUnit.SECONDS, true);
            int threadCount = 25;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            AtomicInteger successCount = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();

            try {
                // Create tasks that will queue up for the lock.
                for (int i = 0; i < threadCount; i++) {
                    futures.add(executor.submit(() -> lock.callWithLock("queue-test", () -> {
                        Thread.sleep(10);
                        successCount.incrementAndGet();
                        return null;
                    })));
                }
                for (Future<?> f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }

                // Verify that all threads managed to obtain the lock.
                assertThat(successCount.get()).isEqualTo(threadCount);
                // Verify the lock node gets cleaned up.
                await().atMost(5, TimeUnit.SECONDS).until(() -> client.checkExists().forPath("/locks/queue-test") == null);
            } finally {
                executor.shutdown();
            }
        }

        /**
         * Verify that obtaining a lock in a reentrant manner for the same lock ID and the same {@link ZkLock} instance is supported.
         */
        @Test
        void reentrantCallFromSameThreadOnSameLockIdSucceeds() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 1, TimeUnit.SECONDS, false);
            String result = lock.callWithLock("reentrant", () -> lock.callWithLock("reentrant", () -> "inner"));
            assertThat(result).isEqualTo("inner");
        }

        /**
         * Verify that nested reentrant calls to the same lock ID and the same {@link ZkLock} can succeed.
         */
        @Test
        void deeplyNestedReentrantCallsAllSucceed() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 1, TimeUnit.SECONDS, false);
            AtomicInteger depth = new AtomicInteger();
            AtomicInteger maxDepth = new AtomicInteger();

            lock.callWithLock("nested-depth", () -> {
                depth.incrementAndGet();
                maxDepth.updateAndGet(m -> Math.max(m, depth.get()));
                return lock.callWithLock("nested-depth", () -> {
                    depth.incrementAndGet();
                    maxDepth.updateAndGet(m -> Math.max(m, depth.get()));
                    return lock.callWithLock("nested-depth", () -> {
                        depth.incrementAndGet();
                        maxDepth.updateAndGet(m -> Math.max(m, depth.get()));
                        return null;
                    });
                });
            });

            // Verify all nested calls obtained and released the lock.
            assertThat(maxDepth.get()).isEqualTo(3);
        }

        /**
         * Verify that reentrant support does not span different instances of {@link ZkLock}.
         */
        @Test
        void reentrancyDoesNotSpanDifferentZkLockInstances() {
            // Reentrancy is scoped to a single ZkLock instance's cached mutex. A different ZkLock instance (even for the same path, client, and thread) has its
            // cache entry and must genuinely contend via Zookeeper rather than being recognized as already held.
            ZkLock lockA = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, false);
            ZkLock lockB = new ZkLock(client, "/locks", 500, TimeUnit.MILLISECONDS, false);
            AtomicBoolean lockAAcquired = new AtomicBoolean(false);

            assertThatThrownBy(() -> lockA.callWithLock("cross-scope", () -> {
                // Acquire the lock with ZkLock A, and then attempt to acquire the lock with ZkLock B, this should time out.
                lockAAcquired.set(true);
                assertThat(client.checkExists().forPath("/locks/cross-scope")).isNotNull();
                return lockB.callWithLock("cross-scope", () -> "never");
            })).isInstanceOf(TimeoutException.class);

            // Verify that the lock was acquired in lock A.
            assertThat(lockAAcquired.get()).isTrue();
        }

        /**
         * Verify that the node in Zookeeper is not deleted while an outer reentrant call still holds the lock.
         */
        @Test
        void nodeNotDeletedWhileOuterReentrantFrameStillHoldsLock() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 5, TimeUnit.SECONDS, true);

            lock.callWithLock("reentrant-delete", () -> {
                assertThat(client.checkExists().forPath("/locks/reentrant-delete")).isNotNull();

                lock.callWithLock("reentrant-delete", () -> {
                    // Still exists during the nested, reentrant hold.
                    assertThat(client.checkExists().forPath("/locks/reentrant-delete")).isNotNull();
                    return null;
                });

                // Nested call released, but the outer frame still holds the lock. Node must still exist.
                assertThat(client.checkExists().forPath("/locks/reentrant-delete")).isNotNull();
                return null;
            });

            // Only now that the outer frame has released too should the node be cleaned up.
            await().atMost(3, TimeUnit.SECONDS).until(() -> client.checkExists().forPath("/locks/reentrant-delete") == null);
        }

        /**
         * Verify that nested reentrant holds block other threads until the outer reentrant call releases the lock.
         */
        @Test
        void nestedReentrantHoldBlocksOtherThreadsUntilFullyReleased() throws Exception {
            ZkLock lock = new ZkLock(client, "/locks", 10, TimeUnit.SECONDS, false);
            CountDownLatch innerEntered = new CountDownLatch(1);
            CountDownLatch releaseOuter = new CountDownLatch(1);
            CountDownLatch otherThreadAcquired = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                // Create a task that will create and hold a lock with nested reentrant locks.
                Future<?> holderFuture = executor.submit(() -> {
                    try {
                        lock.callWithLock("nested-contend", () -> lock.callWithLock("nested-contend", () -> {
                            innerEntered.countDown();
                            releaseOuter.await();
                            return null;
                        }));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // Verify we acquired the lock in the nested call.
                assertThat(innerEntered.await(5, TimeUnit.SECONDS)).isTrue();

                // Crete a task to obtain the lock on a different thread.
                Future<?> otherFuture = executor.submit(() -> {
                    try {
                        lock.callWithLock("nested-contend", () -> {
                            otherThreadAcquired.countDown();
                            return null;
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // Must not acquire while either the outer or nested frame on the holder thread is still held.
                assertThat(otherThreadAcquired.await(1, TimeUnit.SECONDS)).isFalse();

                // Release the outer reentrant hold.
                releaseOuter.countDown();
                holderFuture.get(5, TimeUnit.SECONDS);

                // Verify the second process was able to obtain the lock.
                assertThat(otherThreadAcquired.await(5, TimeUnit.SECONDS)).isTrue();
                otherFuture.get(5, TimeUnit.SECONDS);
            } finally {
                executor.shutdown();
            }
        }

        /**
         * Verify that even when we have multiple {@link ZkLock} instances with the same root path and client, they still mutually exclude correctly on the same
         * lock ID.
         */
        @Test
        void concurrentDistinctLockUsersDoNotCorruptSharedRootPathState() throws Exception {
            ZkLock lockA = new ZkLock(client, "/locks", 10, TimeUnit.SECONDS, false);
            ZkLock lockB = new ZkLock(client, "/locks", 10, TimeUnit.SECONDS, false);
            AtomicInteger counter = new AtomicInteger();
            AtomicInteger maxConcurrent = new AtomicInteger();
            AtomicInteger currentConcurrent = new AtomicInteger();
            int iterations = 20;
            ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                // Create tasks to obtain a lock with ZkLock A.
                Future<?> futureA = executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        lockA.callWithLock("cross-instance", () -> {
                            int c = currentConcurrent.incrementAndGet();
                            maxConcurrent.updateAndGet(m -> Math.max(m, c));
                            Thread.sleep(2);
                            counter.incrementAndGet();
                            currentConcurrent.decrementAndGet();
                            return null;
                        });
                    }
                    return null;
                });

                // Create tasks to obtain a lock with ZKLock B.
                Future<?> futureB = executor.submit(() -> {
                    for (int i = 0; i < iterations; i++) {
                        lockB.callWithLock("cross-instance", () -> {
                            int c = currentConcurrent.incrementAndGet();
                            maxConcurrent.updateAndGet(m -> Math.max(m, c));
                            Thread.sleep(2);
                            counter.incrementAndGet();
                            currentConcurrent.decrementAndGet();
                            return null;
                        });
                    }
                    return null;
                });

                // Wait for the tasks to complete.
                futureA.get(30, TimeUnit.SECONDS);
                futureB.get(30, TimeUnit.SECONDS);

                // Verify that all tasks managed to obtain the lock, but that only one thread ever held the lock at /locks/cross-instance at the same time.
                assertThat(counter.get()).isEqualTo(iterations * 2);
                assertThat(maxConcurrent.get()).isEqualTo(1);
            } finally {
                executor.shutdown();
            }
        }
    }
}
