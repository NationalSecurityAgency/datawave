package datawave.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * A suite of unit tests that compare the performance of different cache implementations in various configurations
 */
public class CVCacheIT {

    private final boolean debug = false;
    private final int maxIterations = 1_000;
    private final int lowThreads = 5;
    private final int highThreads = 15;

    @Test
    public void testSimpleVisibility() {
        ByteSequence sequence = get("PUBLIC");
        testSingleThread(List.of(sequence));
    }

    @Test
    public void testComplexVisibility() {
        ByteSequence sequence = get("PUBLIC&FOO&(BAR|BAZ)");
        testSingleThread(List.of(sequence));
    }

    @Test
    public void testVariableColumnVisibilities() {
        List<ByteSequence> sequences = List.of(get("PUBLIC"), get("PRIVATE"), get("PUBLIC&FOO&(BAR|BAZ)"), get("FIZZ|BUZZ"));
        testSingleThread(sequences);
    }

    @Test
    public void testInvalidColumnVisibility() {
        String visibility = "PUBLIC&)|";
        int count = 0;
        List<CVCache> caches = getCaches();
        for (CVCache cache : caches) {
            ByteSequence sequence = get(visibility);
            try {
                cache.get(sequence);
            } catch (Exception e) {
                boolean isIllegalArgumentException = e instanceof IllegalArgumentException;
                boolean isUncheckedExecutionException = e instanceof UncheckedExecutionException;
                assertTrue(isIllegalArgumentException || isUncheckedExecutionException);
            }
            count++;
        }
        assertEquals(count, caches.size());
    }

    /**
     * Drive the input byte sequences against each cache implementation
     *
     * @param sequences
     *            the list of input byte sequences
     */
    protected void testSingleThread(List<ByteSequence> sequences) {
        List<CVCache> caches = getCaches();
        List<Pair<Long,String>> outputs = new ArrayList<>();
        for (CVCache cache : caches) {
            Pair<Long,String> output = testSingleThread(cache, sequences);
            outputs.add(output);
        }

        if (debug) {
            Collections.sort(outputs);
            for (Pair<Long,String> output : outputs) {
                System.out.println(output.getRight());
            }
        }
    }

    /**
     * Drive the input byte sequences against the provided {@link CVCache}
     *
     * @param cache
     *            a specific {@link CVCache} implementation
     * @param sequences
     *            the list of input byte sequences
     * @return the total time to access the cache with a stats line
     */
    protected Pair<Long,String> testSingleThread(CVCache cache, List<ByteSequence> sequences) {
        long total = 0L;
        List<ColumnVisibility> expected = new ArrayList<>();
        List<ColumnVisibility> results = new ArrayList<>();
        for (int i = 0; i < maxIterations; i++) {
            for (ByteSequence sequence : sequences) {
                long elapsed = System.nanoTime();
                ColumnVisibility result = cache.get(sequence);
                total += System.nanoTime() - elapsed;
                ColumnVisibility expectation = new ColumnVisibility(sequence.toArray());
                results.add(result);
                expected.add(expectation);
            }
            assertEquals(expected, results);
        }

        String line = "T=1 " + maxIterations + " took " + total + " ns for " + cache.name();
        return Pair.of(total, line);
    }

    @Test
    public void testLowThreadsLowVariance() {
        List<ByteSequence> sequences = getRandomSequences(10);
        testThreads(lowThreads, sequences);
    }

    @Test
    public void testHighThreadsLowVariance() {
        List<ByteSequence> sequences = getRandomSequences(10);
        testThreads(highThreads, sequences);
    }

    @Test
    public void testLowThreadsHighVariance() {
        List<ByteSequence> sequences = getRandomSequences(100);
        testThreads(lowThreads, sequences);
    }

    @Test
    public void testHighThreadsHighVariance() {
        List<ByteSequence> sequences = getRandomSequences(100);
        testThreads(highThreads, sequences);
    }

    /**
     * Drive the input byte sequences against the provided {@link CVCache} with a number of threads
     *
     * @param threads
     *            the number of threads to use
     * @param sequences
     *            the list of input byte sequences
     */
    protected void testThreads(int threads, List<ByteSequence> sequences) {
        List<CVCache> caches = getCaches();
        List<Pair<Long,String>> outputs = new ArrayList<>();
        for (CVCache cache : caches) {
            Pair<Long,String> output = testThreads(threads, cache, sequences);
            outputs.add(output);
        }

        if (debug) {
            Collections.sort(outputs);
            for (Pair<Long,String> output : outputs) {
                System.out.println(output.getRight());
            }
        }
    }

    /**
     * Drive the input byte sequences against the provided {@link CVCache} with a number of threads
     *
     * @param threads
     *            the number of threads to use
     * @param cache
     *            a specific {@link CVCache} implementation
     * @param sequences
     *            the list of input byte sequences
     * @return the total time to access the cache across all threads with a stats line
     */
    protected Pair<Long,String> testThreads(int threads, CVCache cache, List<ByteSequence> sequences) {
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        List<Callable<Long>> callables = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final List<ByteSequence> copy = new ArrayList<>(sequences);
            Collections.shuffle(copy);
            callables.add(() -> {
                long total = 0L;
                List<ColumnVisibility> expected = new ArrayList<>();
                List<ColumnVisibility> results = new ArrayList<>();
                for (int k = 0; k < maxIterations; k++) {
                    for (ByteSequence sequence : copy) {
                        long elapsed = System.nanoTime();
                        ColumnVisibility result = cache.get(sequence);
                        total += System.nanoTime() - elapsed;

                        results.add(result);
                        expected.add(new ColumnVisibility(sequence.toArray()));
                    }
                    assertEquals(expected, results);
                }
                return total;
            });
        }

        try {
            List<Future<Long>> futures = executor.invokeAll(callables);

            long total = 0L;
            for (Future<Long> future : futures) {
                long time = future.get();
                total += time;
            }
            String line = "T=" + threads + " " + maxIterations + " took " + total + " ns for " + cache.name();
            return Pair.of(total, line);

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    protected List<ByteSequence> getRandomSequences(int n) {
        List<ByteSequence> sequences = new ArrayList<>();
        while (sequences.size() < n) {
            String data = RandomStringUtils.secure().nextAlphabetic(10);
            sequences.add(get(data));
        }
        return sequences;
    }

    protected ArrayByteSequence get(String s) {
        return new ArrayByteSequence(s);
    }

    /**
     * Get a list of each unique cache implementation in random order
     *
     * @return the list of caches
     */
    protected List<CVCache> getCaches() {
        List<CVCache> caches = new ArrayList<>();
        caches.add(new GuavaCVCache());
        caches.add(new CaffeineCVCache());
        caches.add(new LRUMapCVCache());

        Collections.shuffle(caches);
        return caches;
    }

}
