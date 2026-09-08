package datawave.query.jexl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.fst.FST;
import org.junit.jupiter.api.Test;

import datawave.core.iterators.DatawaveFieldIndexListIteratorJexl;

class DatawaveArithmeticTest {

    private static final int THREADS = 8;
    private static final int LOOKUPS_PER_THREAD = 2000;

    private static final String[] PRESENT = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"};
    private static final String[] ABSENT = {"alph", "alphaa", "bravo1", "india", "juliett", "", "zulu", "Charlie"};

    @Test
    void testMatchesFst() throws Exception {
        FST<?> fst = buildFst();

        for (String value : PRESENT) {
            assertTrue(DatawaveArithmetic.matchesFst(value, fst), "expected a match for " + value);
        }
        for (String value : ABSENT) {
            assertFalse(DatawaveArithmetic.matchesFst(value, fst), "expected no match for " + value);
        }
    }

    /**
     * Hammers a single shared FST from several threads at once. This would also pass while matchesFst held a lock -- its purpose is to guard the correctness of
     * the unlocked lookup path, so that the JVM-wide monitor that used to serialize every scan thread is not reintroduced.
     */
    @Test
    void testMatchesFstIsThreadSafe() throws Exception {
        final FST<?> fst = buildFst();
        final CountDownLatch startGate = new CountDownLatch(1);
        final ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        try {
            List<Future<Integer>> futures = new ArrayList<>(THREADS);
            for (int t = 0; t < THREADS; t++) {
                futures.add(executor.submit(() -> {
                    startGate.await();
                    int lookups = 0;
                    for (int i = 0; i < LOOKUPS_PER_THREAD; i++) {
                        String present = PRESENT[i % PRESENT.length];
                        String absent = ABSENT[i % ABSENT.length];
                        assertTrue(DatawaveArithmetic.matchesFst(present, fst), "expected a match for " + present);
                        assertFalse(DatawaveArithmetic.matchesFst(absent, fst), "expected no match for " + absent);
                        lookups++;
                    }
                    return lookups;
                }));
            }

            startGate.countDown();
            executor.shutdown();
            assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS), "threads did not finish in time");

            // get() rethrows anything that escaped a worker
            for (Future<Integer> future : futures) {
                assertEquals(LOOKUPS_PER_THREAD, future.get().intValue());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private FST<?> buildFst() throws Exception {
        SortedSet<String> values = new TreeSet<>();
        Collections.addAll(values, PRESENT);
        return DatawaveFieldIndexListIteratorJexl.getFST(values);
    }
}
