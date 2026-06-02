package datawave.query.iterator.profile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class QuerySpanTest {

    @Test
    public void testQuerySpanAggregation() {

        QuerySpan qs1 = new QuerySpan(null);
        advanceIterators(qs1);

        Assert.assertEquals(7, qs1.getSeekCount());
        Assert.assertEquals(16, qs1.getNextCount());
        Assert.assertEquals(4, qs1.getSourceCount());
        Assert.assertTrue(qs1.getYield());
    }

    @Test
    public void testQuerySpanAggregationWithoutYielding() {

        QuerySpan qs1 = new QuerySpan(null);
        advanceIteratorsWithoutYield(qs1);

        Assert.assertEquals(1, qs1.getSeekCount());
        Assert.assertEquals(3, qs1.getNextCount());
        Assert.assertEquals(1, qs1.getSourceCount());
        Assert.assertFalse(qs1.getYield());
    }

    @Test
    public void testMultiThreadedQuerySpanAggregation() {

        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(null, null);
        advanceIterators(qs1);

        // this works because we are in the same thread as when advanceIterators was called
        Assert.assertEquals(7, qs1.getSeekCount());
        Assert.assertEquals(16, qs1.getNextCount());
        Assert.assertEquals(4, qs1.getSourceCount());
        Assert.assertTrue(qs1.getYield());
    }

    @Test
    public void testMultiThreadedQuerySpanCollection() {

        QuerySpanCollector qsc = new QuerySpanCollector();
        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs1);
        MultiThreadedQuerySpan qs2 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs2);
        MultiThreadedQuerySpan qs3 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs3);

        // this works because we are in the same thread as when advanceIterators was called
        QuerySpan qs4 = qsc.getCombinedQuerySpan(null, true);

        Assert.assertEquals(21, qs4.getSeekCount());
        Assert.assertEquals(48, qs4.getNextCount());
        Assert.assertEquals(12, qs4.getSourceCount());
        Assert.assertTrue(qs4.getYield());
    }

    @Test
    public void testMultiThreadedQuerySpanAcrossThreads() {

        QuerySpanCollector qsc = new QuerySpanCollector();
        // call advanceIterators for each MultiThreadedQuerySpan in the main thread
        // counts are collected in the QuerySpanCollector
        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs1);
        MultiThreadedQuerySpan qs2 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs2);
        MultiThreadedQuerySpan qs3 = new MultiThreadedQuerySpan(qsc, null);
        advanceIterators(qs3);

        // call advanceIterators for each MultiThreadedQuerySpan in separate threads
        // counts are collected in the QuerySpanCollector
        Runnable r1 = new QSRunnable(qs1);
        Runnable r2 = new QSRunnable(qs2);
        Runnable r3 = new QSRunnable(qs3);
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("QSExecutor-%d").build();
        ExecutorService executorService = Executors.newFixedThreadPool(10, tf);

        executorService.execute(r1);
        executorService.execute(r2);
        executorService.execute(r3);
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // the QuerySpanCollector should have counts for calling advanceIterators
        // 3 times on the main thread plus one time each in 3 separate threads
        // seeks = 7 * 6 = 42
        // next = 16 * 6 = 96
        // sources = 4 * 6 = 24
        QuerySpan qs4 = qsc.getCombinedQuerySpan(null, true);

        Assert.assertEquals(42, qs4.getSeekCount());
        Assert.assertEquals(96, qs4.getNextCount());
        Assert.assertEquals(24, qs4.getSourceCount());
        Assert.assertTrue(qs4.getYield());
    }

    private class QSRunnable implements Runnable {

        private QuerySpan querySpan;

        public QSRunnable(QuerySpan querySpan) {
            this.querySpan = querySpan;
        }

        @Override
        public void run() {
            advanceIterators(this.querySpan);
            try {
                Thread.sleep(250L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void advanceIteratorsWithoutYield(QuerySpan qs1) {
        qs1.seek();
        qs1.next();
        qs1.next();
        qs1.next();
    }

    private void advanceIterators(QuerySpan qs1) {
        qs1.seek();
        qs1.next();
        qs1.next();
        qs1.next();
        QuerySpan qs2 = qs1.createSource();
        qs2.seek();
        qs2.next();
        qs2.next();
        qs2.next();
        qs2.next();
        qs2.yield();
        QuerySpan qs3 = qs1.createSource();
        qs3.seek();
        qs3.seek();
        qs3.seek();
        qs3.seek();
        qs3.next();
        qs3.next();
        qs3.next();
        qs3.next();
        qs3.next();
        qs3.yield();
        QuerySpan qs4 = qs3.createSource();
        qs4.seek();
        qs4.next();
        qs4.next();
        qs4.next();
        qs4.next();
        qs4.yield();
    }
}
