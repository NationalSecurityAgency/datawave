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
    }

    @Test
    public void queueCapacityBlockTest() throws InterruptedException {
        // test that the producer will block until capacity becomes available

        // buffer size is 2
        builder.setMaxRanges(2);

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
        long delay = 25;

        builder.setMaxRanges(1);

        // add a tiny delay to ensure that there is time to get inside the while loop before the rangeConsumer finishes
        // this is necessary to prevent the test from intermittently failing on some hardware and jvms
        expect(mockPlans.iterator()).andReturn(delayIterator(Collections.emptyIterator(), delay));

        mockPlans.close();
        expectLastCall().times(0, 1);

        replayAll();

        ThreadedRangeBundlerIterator itr = builder.build();
        long start = System.currentTimeMillis();
        assertFalse(itr.hasNext());
        long end = System.currentTimeMillis();
        assertTrue(end - start >= delay);
        // really this should be maxWaitValue+1, but cpu speeds and scheduling may cause intermittent failures then
        assertTrue(end - start < 2 * delay);
        verifyAll();
    }

    @Test
    public void disableBundlingFastProducerTest() throws IOException {
        int maxWaitValue = 50;
        builder.setMaxRanges(1);

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
        int delay = 200;

        Iterator<QueryPlan> itr = plans.iterator();

        // simulate a delay on producing each range
        Iterator<QueryPlan> wrapped = delayIterator(itr, delay);

        expect(mockPlans.iterator()).andReturn(wrapped);

        replayAll();

        ThreadedRangeBundlerIterator trbi = builder.build();
        long start = System.currentTimeMillis();
        // because the ranges take 200ms and numRangesToBuffer = 2 it will sleep the poll time
        assertTrue(trbi.hasNext());
        long end = System.currentTimeMillis();
        assertTrue(end - start >= delay);
        trbi.next();
        long start2 = System.currentTimeMillis();
        assertTrue(trbi.hasNext());
        long end2 = System.currentTimeMillis();
        // this happened async, so should be faster, but not more than the delay
        assertTrue((end2 - start2) <= delay);

        long total = (System.currentTimeMillis() - start);
        // total time will be the sum of the delay + overhead
        assertTrue(total >= delay * 2);

        verifyAll();
    }

    @Test
    public void failFastOnExhaustedTest() throws IOException {
        expect(mockPlans.iterator()).andReturn(Collections.emptyIterator());
        mockPlans.close();

        replayAll();

        ThreadedRangeBundlerIterator trbi = builder.build();
        long start = System.currentTimeMillis();
        assertFalse(trbi.hasNext());
        long end = System.currentTimeMillis();
        // arbitrary fast time less than any previous poll time, actual time probably 1 but to keep this unit test predictable
        assertTrue(end - start < 5);

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
