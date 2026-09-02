package datawave.query.tables.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.Transformer;
import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import datawave.core.iterators.ResultCountingIterator.ResultCountTuple;
import datawave.marking.MarkingFunctions;

/**
 * Tests the aggregation performed by the {@link CountAggregatingIterator} and the lifecycle of the thread that performs it.
 */
public class CountAggregatingIteratorTest {

    /** the name given to the thread that performs the aggregation */
    private static final String AGGREGATION_THREAD_NAME = "count aggregation";

    /** long enough for a thread that is going to exit to have exited */
    private static final long THREAD_EXIT_WAIT_MILLIS = 5_000L;

    private final Clock clock = Clock.systemUTC();

    private final MarkingFunctions<?> markingFunctions = new MarkingFunctions.Default();

    /** returns the aggregated (count, visibility) pair untouched so tests can assert against it */
    private final Transformer transformer = input -> input;

    @Test
    public void testCountsAreAggregatedAcrossAllEntries() {
        CountAggregatingIterator iterator = new CountAggregatingIterator(entries(3L, 5L, 11L), transformer, markingFunctions, THREAD_EXIT_WAIT_MILLIS);

        assertEquals(19L, drainToCount(iterator));
    }

    /**
     * A fully consumed iterator has no more aggregation to do, so the aggregation thread must exit on its own.
     */
    @Test
    public void testAggregationThreadExitsWithFullyConsumedIterator() {
        CountAggregatingIterator iterator = new CountAggregatingIterator(entries(3L, 5L), transformer, markingFunctions, THREAD_EXIT_WAIT_MILLIS);
        assertEquals(8L, drainToCount(iterator));

        assertTrue(awaitAggregationThreadsExit(), "aggregation thread outlived the fully consumed iterator");
    }

    /**
     * A query closed while aggregation is still blocked on the source must release the aggregation thread.
     */
    @Test
    public void testCloseReleasesThreadBlockedOnSource() throws Exception {
        CountDownLatch blocked = new CountDownLatch(1);
        CountAggregatingIterator iterator = new CountAggregatingIterator(blockingEntries(blocked), transformer, markingFunctions, 10L);

        // the aggregation thread is now parked in hasNext(), so the first page is an intermediate result
        assertTrue(blocked.await(THREAD_EXIT_WAIT_MILLIS, TimeUnit.MILLISECONDS), "aggregation thread never reached the source iterator");
        assertTrue(iterator.hasNext());
        assertTrue(countAggregationThreads() >= 1, "expected an aggregation thread to be running");

        iterator.close();

        assertTrue(awaitAggregationThreadsExit(), "aggregation thread outlived the closed iterator");
    }

    /**
     * Consume the iterator, discarding intermediate results, and return the aggregated count.
     *
     * @param iterator
     *            the iterator under test
     * @return the aggregated count
     */
    @SuppressWarnings("unchecked")
    private long drainToCount(CountAggregatingIterator iterator) {
        Object last = null;
        while (iterator.hasNext()) {
            last = iterator.next();
        }
        return ((Entry<Long,ColumnVisibility>) last).getKey();
    }

    /**
     * Build an iterator of entries, one per provided count, in the serialized form the count iterator stack produces.
     *
     * @param counts
     *            the count carried by each entry
     * @return an iterator of entries
     */
    private Iterator<Entry<Key,Value>> entries(long... counts) {
        List<Entry<Key,Value>> entries = new ArrayList<>();
        for (long count : counts) {
            entries.add(Map.entry(new Key("row"), serialize(count)));
        }
        return entries.iterator();
    }

    /**
     * Build an iterator that never produces an entry and only returns from {@link Iterator#hasNext()} when interrupted, standing in for a scan that is still
     * running when the query is closed.
     *
     * @param blocked
     *            counted down once the caller is about to block
     * @return an iterator that blocks until interrupted
     */
    private Iterator<Entry<Key,Value>> blockingEntries(CountDownLatch blocked) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                blocked.countDown();
                try {
                    // a scan that outlives the query
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return false;
            }

            @Override
            public Entry<Key,Value> next() {
                return null;
            }
        };
    }

    private Value serialize(long count) {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        kryo.writeObject(output, new ResultCountTuple(count, new ColumnVisibility()));
        output.close();
        return new Value(baos.toByteArray());
    }

    /**
     * @return the number of live aggregation threads
     */
    private long countAggregationThreads() {
        // @formatter:off
        return Thread.getAllStackTraces().keySet()
                        .stream()
                        .filter(Thread::isAlive)
                        .filter(t -> AGGREGATION_THREAD_NAME.equals(t.getName()))
                        .count();
        // @formatter:on
    }

    /**
     * Wait for every aggregation thread to exit.
     *
     * @return true if no aggregation thread is left running
     */
    private boolean awaitAggregationThreadsExit() {
        long deadline = clock.millis() + THREAD_EXIT_WAIT_MILLIS;
        while (clock.millis() < deadline) {
            if (countAggregationThreads() == 0) {
                return true;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return countAggregationThreads() == 0;
    }
}
