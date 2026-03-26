package datawave.query.tables.shard;

import static datawave.core.iterators.ResultCountingIterator.ResultCountTuple;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Maps;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.result.event.DefaultEvent;

/**
 * A transformer that aggregates query results into a count.
 * <p>
 * Aggregation happens in a separate thread so that an intermediate result can be returned to the RunningQuery.
 */
public class CountAggregatingIterator extends TransformIterator {
    private static final Logger log = Logger.getLogger(CountAggregatingIterator.class);

    private static final long DEFAULT_PAGE_WAIT_TIME_MILLIS = 3_600_000L;

    private boolean done = false;
    private final AtomicBoolean executing = new AtomicBoolean(true);
    private final CountDownLatch latch = new CountDownLatch(1);

    private final long pageWaitTimeMillis;

    private final CountEntryAggregator aggregator;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    /**
     * Constructor with that uses {@link #DEFAULT_PAGE_WAIT_TIME_MILLIS}
     *
     * @param iterator
     *            the iterator
     * @param transformer
     *            the transformer
     * @param markingFunctions
     *            the marking functions
     */
    public CountAggregatingIterator(Iterator<Entry<Key,Value>> iterator, Transformer transformer, MarkingFunctions markingFunctions) {
        this(iterator, transformer, markingFunctions, DEFAULT_PAGE_WAIT_TIME_MILLIS);
    }

    /**
     * Constructor that uses provided page wait time millis
     *
     * @param iterator
     *            the iterator
     * @param transformer
     *            the transformer
     * @param markingFunctions
     *            the marking functions
     * @param pageWaitTimeMillis
     *            the time to wait for the next page
     */
    @SuppressWarnings("unchecked")
    public CountAggregatingIterator(Iterator<Entry<Key,Value>> iterator, Transformer transformer, MarkingFunctions markingFunctions, long pageWaitTimeMillis) {
        super(iterator, transformer);
        this.aggregator = new CountEntryAggregator(transformer, markingFunctions);
        this.pageWaitTimeMillis = pageWaitTimeMillis;

        CountAggregatingRunnable runnable = new CountAggregatingRunnable(iterator, aggregator, executing, latch);
        executor.execute(runnable);
    }

    @Override
    public boolean hasNext() {
        return !done;
    }

    @Override
    public Object next() {
        try {
            latch.await(pageWaitTimeMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            // nope
        }

        if (executing.get()) {
            return getIntermediateEvent();
        } else {
            done = true;
            return aggregator.getAggregatedEvent();
        }
    }

    /**
     * Build a {@link DefaultEvent} with the intermediate result flag set to true
     *
     * @return an intermediate result
     */
    private Object getIntermediateEvent() {
        DefaultEvent event = new DefaultEvent();
        event.setIntermediateResult(true);
        return event;
    }

    /**
     * Encapsulate aggregation logic here
     */
    private static class CountEntryAggregator {

        private final AtomicLong count = new AtomicLong(0L);
        private final Set<ColumnVisibility> cvs = new HashSet<>();

        private final Transformer transformer;
        private final MarkingFunctions markingFunctions;

        public CountEntryAggregator(Transformer transformer, MarkingFunctions markingFunctions) {
            this.transformer = transformer;
            this.markingFunctions = markingFunctions;
        }

        public void addCount(long count) {
            this.count.addAndGet(count);
        }

        public void addColumnVisibility(ColumnVisibility cv) {
            this.cvs.add(cv);
        }

        @SuppressWarnings("unchecked")
        public Object getAggregatedEvent() {
            if (cvs.isEmpty() && count.get() == 0L) {
                cvs.add(new ColumnVisibility(""));
            }

            ColumnVisibility cv = getCombinedColumnVisibility();
            return transformer.transform(Maps.immutableEntry(count.get(), cv));
        }

        private ColumnVisibility getCombinedColumnVisibility() {
            try {
                return markingFunctions.combine(cvs);
            } catch (Exception e) {
                log.error("Could not combine columnVisibilities for the count", e);
                return null;
            }
        }
    }

    /**
     * A runnable that pulls {@link ResultCountTuple} from the iterator and passes them to the {@link CountEntryAggregator}
     */
    private static class CountAggregatingRunnable implements Runnable {
        private final Kryo kryo = new Kryo();

        private final Iterator<Entry<Key,Value>> iterator;
        private final CountEntryAggregator aggregator;
        private final AtomicBoolean executing;
        private final CountDownLatch latch;

        public CountAggregatingRunnable(Iterator<Entry<Key,Value>> iterator, CountEntryAggregator aggregator, AtomicBoolean executing, CountDownLatch latch) {
            this.iterator = iterator;
            this.aggregator = aggregator;
            this.executing = executing;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                log.info("Beginning count aggregation");
                while (iterator.hasNext()) {
                    Entry<Key,Value> entry = iterator.next();
                    if (null == entry || entry.getKey() == null || entry.getValue() == null) {
                        continue;
                    }

                    // Unpack the kryo serialized object, it contains the count and the accumulated visibility
                    ResultCountTuple tuple = unpackValue(entry.getValue());
                    aggregator.addColumnVisibility(tuple.getVisibility());
                    aggregator.addCount(tuple.getCount());
                }
            } finally {
                log.info("Finished count aggregation");
                executing.set(false);
                latch.countDown();
            }
        }

        /**
         * Deserialize the value
         *
         * @param value
         *            the value
         * @return a {@link ResultCountTuple}
         */
        private ResultCountTuple unpackValue(Value value) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(value.get()); Input input = new Input(bais)) {
                return kryo.readObject(input, ResultCountTuple.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
