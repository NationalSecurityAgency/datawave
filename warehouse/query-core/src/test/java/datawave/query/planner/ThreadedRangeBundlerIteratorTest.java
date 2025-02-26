package datawave.query.planner;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.CloseableIterable;

/**
 * Verify the relationship between configuration and blocking on the producer and consumer side of the ThreadedRangeBundlerIterator
 */
public class ThreadedRangeBundlerIteratorTest extends EasyMockSupport {
    private CloseableIterable<QueryPlan> mockPlans;
    private Query query;
    private QueryData queryData;
    private List<QueryPlan> plans;
    private ThreadedRangeBundlerIterator.Builder builder;

    @Before
    public void setup() {
        // TODO delete this
        // enable to see logs
        // Configurator.setLevel(ThreadedRangeBundlerIterator.class, Level.TRACE);

        mockPlans = createMock(CloseableIterable.class);
        query = new QueryImpl();
        queryData = new QueryData();
        plans = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            QueryPlan plan = new QueryPlan();
            plan.ranges = Collections.emptyList();
            plan.queryTreeString = "abc";
            plans.add(plan);
        }

        // set some defaults
        builder = new ThreadedRangeBundlerIterator.Builder();
        builder.setRanges(mockPlans);
        builder.setSettings(query);
        builder.setOriginal(queryData);
        // total buffer size
        builder.setMaxRanges(1000);
        // time to wait for a value to be available off the queue
        builder.setMaxWaitValue(10);
        // time to wait before applying wait time for numRangesToBuffer before sleeping
        builder.setRangeBufferTimeoutMillis(1);
        // sleep time when waiting for numRangesToBuffer
        builder.setRangeBufferPollMillis(10000);
        builder.setMaxWaitUnit(TimeUnit.MILLISECONDS);
        // ranges to buffer before processing
        builder.setNumRangesToBuffer(2);
    }

    @Test
    public void queueCapacityBlockTest() throws InterruptedException {
        // test that the producer will block until capacity becomes available

        // buffer size is 2
        builder.setMaxRanges(2);

        // do not buffer ranges ever
        builder.setNumRangesToBuffer(0);

        Iterator<QueryPlan> producerItr = plans.iterator();
        // do not limit speed off this iterator
        expect(mockPlans.iterator()).andReturn(producerItr);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        // add a moment for the rangeConsumer to work otherwise the expectation for the iterator may fail
        Thread.sleep(250);

        verifyAll();

        // could not instantly consume the entire producer
        assertTrue(producerItr.hasNext());
    }

    @Test
    public void queueCapacityUnblockedTest() throws InterruptedException, IOException {
        // test that the producer will block until capacity becomes available

        // buffer size to 1000 so all ranges will fit
        builder.setMaxRanges(1000);

        // do not buffer ranges ever
        builder.setNumRangesToBuffer(0);

        Iterator<QueryPlan> producerItr = plans.iterator();
        // do not limit speed off this iterator
        expect(mockPlans.iterator()).andReturn(producerItr);
        mockPlans.close();
        expectLastCall().times(0, 1);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        // add a moment for the rangeConsumer to work otherwise the expectation for the iterator may fail
        Thread.sleep(250);

        verifyAll();

        // the rangeConsumer consumed the entire producer
        assertFalse(producerItr.hasNext());
    }

    @Test
    public void disableBundlingSlowProducerTest() {
        builder.setMaxRanges(1);
        builder.setNumRangesToBuffer(0);
        builder.setMaxWaitValue(100);

        Iterator<QueryPlan> producerItr = plans.iterator();
        producerItr = delayIterator(producerItr, 1000);

        expect(mockPlans.iterator()).andReturn(producerItr);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        long start = System.currentTimeMillis();
        assertTrue(itr.hasNext());
        long end = System.currentTimeMillis();
        assertTrue((end - start) >= 1000);
        assertTrue((end - start) <= 1100);

        verifyAll();
    }

    @Test
    public void disableBundlingMinWaitTimeTest() throws IOException {
        int maxWaitValue = 50;
        builder.setMaxRanges(1);
        builder.setNumRangesToBuffer(0);
        builder.setMaxWaitValue(maxWaitValue);

        // add a tiny delay to ensure that there is time to get inside the while loop before the rangeConsumer finishes
        // this is necessary to prevent the test from intermittently failing on some hardware and jvms
        expect(mockPlans.iterator()).andReturn(delayIterator(Collections.emptyIterator(), 25));

        mockPlans.close();
        expectLastCall().times(0, 1);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        long start = System.currentTimeMillis();
        assertFalse(itr.hasNext());
        long end = System.currentTimeMillis();
        assertTrue(end - start >= maxWaitValue);
        // really this should be maxWaitValue+1, but cpu speeds and scheduling may cause intermittent failures then
        assertTrue(end - start < 2 * maxWaitValue);

        verifyAll();
    }

    @Test
    public void disableBundlingFastProducerTest() throws IOException {
        int maxWaitValue = 50;
        builder.setMaxRanges(1);
        builder.setNumRangesToBuffer(0);
        builder.setMaxWaitValue(maxWaitValue);

        QueryPlan plan = new QueryPlan();
        plan.ranges = Collections.emptyList();
        plan.queryTreeString = "abc";

        expect(mockPlans.iterator()).andReturn(List.of(plan).iterator());
        mockPlans.close();
        expectLastCall().times(0, 1);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        long start = System.currentTimeMillis();
        assertTrue(itr.hasNext());
        long end = System.currentTimeMillis();

        assertTrue(end - start < maxWaitValue);

        verifyAll();
    }

    @Test
    public void blockForNumRangesToBufferTest() {
        int maxPollTime = 1000;
        // sleep time when waiting for numRangesToBuffer
        builder.setRangeBufferPollMillis(maxPollTime);
        // amount of time to consider sleeping
        builder.setRangeBufferTimeoutMillis(750);
        builder.setNumRangesToBuffer(2);

        Iterator<QueryPlan> itr = plans.iterator();
        // simulate a delay on producing each range
        Iterator<QueryPlan> wrapped = delayIterator(itr, 200);

        expect(mockPlans.iterator()).andReturn(wrapped);

        replayAll();

        ThreadedRangeBundlerIterator trbi = builder.build();
        long start = System.currentTimeMillis();
        // because the ranges take 200ms and numRangesToBuffer = 2 it will sleep the poll time
        assertTrue(trbi.hasNext());
        long end = System.currentTimeMillis();
        assertTrue(end - start >= maxPollTime);
        trbi.next();
        start = System.currentTimeMillis();
        assertTrue(trbi.hasNext());
        end = System.currentTimeMillis();
        // the second time will not block because the initial bundling time has already been exceeded
        assertTrue(end - start < maxPollTime);

        verifyAll();
    }

    private Iterator<QueryPlan> delayIterator(Iterator<QueryPlan> itr, long delay) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return itr.hasNext();
            }

            @Override
            public QueryPlan next() {
                return itr.next();
            }
        };
    }
}
