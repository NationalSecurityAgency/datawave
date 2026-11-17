package datawave.next.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link DocumentSchedulerStats} tolerates concurrent merges.
 * <p>
 * Search threads and retrieval threads merge into a single shared instance while the query thread reads it. Unsynchronized merging loses updates on the plain
 * long counters and can corrupt the array backing a {@link org.apache.commons.math3.stat.descriptive.DescriptiveStatistics}, which is documented as not thread
 * safe.
 */
public class DocumentSchedulerStatsConcurrencyTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static final int THREADS = 8;
    private static final int MERGES_PER_THREAD = 2_000;

    private static final Pattern NEXT_SEEK = Pattern.compile("total next/seek calls: \\((\\d+)/(\\d+)\\)");

    private static final Pattern CANDIDATE_SCAN = Pattern.compile("candidate scan stats: min: (-?\\d+), avg: (-?\\d+), max: (-?\\d+)");

    /**
     * Every merged count must be accounted for. Lost updates show up as a total below the expected value.
     */
    @Test
    public void testConcurrentIteratorStatsMergesDoNotLoseUpdates() throws Exception {
        DocumentSchedulerStats stats = new DocumentSchedulerStats();

        runConcurrently(() -> {
            DocIterStats increment = new DocIterStats();
            increment.incrementNextCount();
            increment.incrementSeekCount();
            stats.merge(increment);
        });

        long expected = (long) THREADS * MERGES_PER_THREAD;

        Matcher matcher = NEXT_SEEK.matcher(stats.logStats("concurrency-test"));
        assertTrue(matcher.find(), "expected the next/seek line to be present");
        assertEquals(expected, Long.parseLong(matcher.group(1)), "next count lost updates under concurrent merge");
        assertEquals(expected, Long.parseLong(matcher.group(2)), "seek count lost updates under concurrent merge");
    }

    /**
     * The candidate stats are backed by a DescriptiveStatistics, which throws or silently corrupts if written concurrently.
     */
    @Test
    public void testConcurrentCandidateStatsMergesDoNotCorruptState() throws Exception {
        DocumentSchedulerStats stats = new DocumentSchedulerStats();

        // every merged sample reports the same scan time, so the reservoir must report exactly that for min, avg and
        // max. A corrupted backing array shows up as a different figure rather than only as an exception
        runConcurrently(() -> {
            DocIdQueryIterStats candidate = new DocIdQueryIterStats();
            candidate.markScanInit(0L);
            candidate.markScanStop(7L);
            candidate.incrementTotalDocumentIds(5);
            stats.merge(candidate);
        });

        String logged = stats.logStats("concurrency-test");
        assertNotNull(logged);

        Matcher matcher = CANDIDATE_SCAN.matcher(logged);
        assertTrue(matcher.find(), "expected the candidate scan stats line to be present");
        assertEquals("7", matcher.group(1), "min scan time was corrupted by concurrent merges");
        assertEquals("7", matcher.group(2), "average scan time was corrupted by concurrent merges");
        assertEquals("7", matcher.group(3), "max scan time was corrupted by concurrent merges");
    }

    /**
     * Retrieval timing stats are merged by every retrieval thread while the query thread reads them.
     */
    @Test
    public void testConcurrentRetrievalStatsMergesAreSafe() throws Exception {
        DocumentSchedulerStats stats = new DocumentSchedulerStats();

        runConcurrently(() -> {
            ScanTimeStats scanTime = new ScanTimeStats();
            scanTime.setContext("row datatype");
            scanTime.markSubmit();
            scanTime.markStart();
            scanTime.markStop();
            stats.merge(scanTime);
        });

        String logged = stats.logStats("concurrency-test");
        assertTrue(logged.contains("retrieval scan stats:"), "retrieval stats must still be readable");
    }

    /**
     * Reading the stats while they are being merged must not observe a torn or corrupt state.
     */
    @Test
    public void testReadingWhileMergingIsSafe() throws Exception {
        DocumentSchedulerStats stats = new DocumentSchedulerStats();
        AtomicReference<Throwable> readerFailure = new AtomicReference<>();

        Thread reader = new Thread(() -> {
            try {
                for (int i = 0; i < 5_000; i++) {
                    stats.logStats("concurrency-test");
                }
            } catch (Throwable t) {
                readerFailure.set(t);
            }
        });
        reader.start();

        runConcurrently(() -> {
            DocIterStats increment = new DocIterStats();
            increment.incrementNextCount();
            stats.merge(increment);
        });

        reader.join(TimeUnit.SECONDS.toMillis(30));
        assertNull(readerFailure.get(), "reading stats concurrently must not fail");
    }

    /**
     * Run the given action on several threads at once, released from a common starting gate to maximise overlap.
     *
     * @param action
     *            the action to repeat on each thread
     * @throws Exception
     *             if a worker thread did not complete
     */
    private void runConcurrently(Runnable action) throws Exception {
        CountDownLatch startGate = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int i = 0; i < THREADS; i++) {
            Thread thread = new Thread(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < MERGES_PER_THREAD; j++) {
                        action.run();
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            thread.start();
            threads.add(thread);
        }

        assertTimeoutPreemptively(TIMEOUT, () -> {
            startGate.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
        });

        if (failure.get() != null) {
            throw new AssertionError("a merging thread failed", failure.get());
        }
    }
}
