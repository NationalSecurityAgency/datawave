package datawave.query.iterator.profile;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.SimpleThreadPool;
import org.junit.Assert;
import org.junit.Test;

public class QuerySpanTest {
    
    @Test
    public void testQuerySpanAggregation() {
        
        QuerySpan qs1 = new QuerySpan(null);
        advanceIterators(qs1);
        
        Assert.assertEquals(7, qs1.getSeekCount());
        Assert.assertEquals(16, qs1.getNextCount());
        Assert.assertTrue(qs1.getYield());
        Assert.assertEquals(4, qs1.getSourceCount());
    }
    
    @Test
    public void testQuerySpanAggregationWithoutYielding() {
        
        QuerySpan qs1 = new QuerySpan(null);
        advanceIteratorsWithoutYield(qs1);
        
        Assert.assertEquals(1, qs1.getSeekCount());
        Assert.assertEquals(3, qs1.getNextCount());
        Assert.assertFalse(qs1.getYield());
        Assert.assertEquals(1, qs1.getSourceCount());
    }
    
    @Test
    public void testMultiThreadedQuerySpanAggregation() {
        
        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs1);
        
        Assert.assertEquals(7, qs1.getSeekCount());
        Assert.assertEquals(16, qs1.getNextCount());
        Assert.assertTrue(qs1.getYield());
        Assert.assertEquals(4, qs1.getSourceCount());
    }
    
    @Test
    public void testMultiThreadedQuerySpanCollection() {
        
        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs1);
        MultiThreadedQuerySpan qs2 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs2);
        MultiThreadedQuerySpan qs3 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs3);
        
        QuerySpanCollector qsc = new QuerySpanCollector();
        qsc.addQuerySpan(qs1);
        qsc.addQuerySpan(qs2);
        qsc.addQuerySpan(qs3);
        QuerySpan qs4 = qsc.getCombinedQuerySpan(null);
        
        Assert.assertEquals(21, qs4.getSeekCount());
        Assert.assertEquals(48, qs4.getNextCount());
        Assert.assertTrue(qs4.getYield());
        Assert.assertEquals(12, qs4.getSourceCount());
    }
    
    @Test
    public void testMultiThreadedQuerySpanAcrossThreads() {
        
        MultiThreadedQuerySpan qs1 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs1);
        MultiThreadedQuerySpan qs2 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs2);
        MultiThreadedQuerySpan qs3 = new MultiThreadedQuerySpan(null);
        advanceIterators(qs3);
        
        QuerySpanCollector qsc = new QuerySpanCollector();
        
        Runnable r1 = new QSRunnable(qsc, qs1);
        Runnable r2 = new QSRunnable(qsc, qs2);
        Runnable r3 = new QSRunnable(qsc, qs3);
        
        ExecutorService executorService = new SimpleThreadPool(10, "QSExecutor");
        executorService.execute(r1);
        executorService.execute(r2);
        executorService.execute(r3);
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        QuerySpan qs4 = qsc.getCombinedQuerySpan(null);
        
        Assert.assertEquals(21, qs4.getSeekCount());
        Assert.assertEquals(48, qs4.getNextCount());
        Assert.assertTrue(qs4.getYield());
        Assert.assertEquals(12, qs4.getSourceCount());
    }
    
    private class QSRunnable implements Runnable {
        
        private QuerySpan querySpan = null;
        private QuerySpanCollector querySpanCollector = null;
        
        public QSRunnable(QuerySpanCollector querySpanCollector, QuerySpan querySpan) {
            this.querySpanCollector = querySpanCollector;
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
            this.querySpanCollector.addQuerySpan(this.querySpan);
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
