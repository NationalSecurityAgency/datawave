package datawave.query.tracking;

import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.profile.QuerySpan;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ActiveQueryLogTest {
    
    private static Random rand = new Random();
    
    private static String createQueryId() {
        return (RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils
                        .randomAlphanumeric(5)).toUpperCase();
    }
    
    private static long createRandomDelay() {
        return rand.nextInt(3) + 1;
    }
    
    public static class QueryTask implements Runnable {
        private final String queryId;
        private final int numSeeks;
        private final int numNexts;
        private final boolean randomSleep;
        
        public QueryTask(String queryId, int numSeeks, int numNexts, boolean randomSleep) {
            this.queryId = queryId;
            this.numSeeks = numSeeks;
            this.numNexts = numNexts;
            this.randomSleep = randomSleep;
        }
        
        public void run() {
            Range r1 = new Range(new Key("a"), new Key("b"));
            Range r2 = new Range(new Key("b"), new Key("c"));
            Range r3 = new Range(new Key("c"), new Key("d"));
            Range r4 = new Range(new Key("d"), new Key("e"));
            Range r5 = new Range(new Key("e"), new Key("f"));
            Range r6 = new Range(new Key("f"), new Key("g"));
            Range[] rangeArray = new Range[6];
            rangeArray[0] = r1;
            rangeArray[1] = r2;
            rangeArray[2] = r3;
            rangeArray[3] = r4;
            rangeArray[4] = r5;
            rangeArray[5] = r6;
            
            try {
                QuerySpan qs = new QuerySpan(null);
                for (int seek = 0; seek < numSeeks; seek++) {
                    Range range = rangeArray[seek % rangeArray.length];
                    ActiveQueryLog.getInstance().get(queryId).beginCall(range, ActiveQuery.CallType.SEEK);
                    if (randomSleep) {
                        TimeUnit.SECONDS.sleep(createRandomDelay());
                    }
                    ActiveQueryLog.getInstance().get(queryId).endCall(range, ActiveQuery.CallType.SEEK);
                    qs.seek();
                    qs.seek();
                }
                
                for (int next = 0; next < numNexts; next++) {
                    Range range = rangeArray[next % rangeArray.length];
                    ActiveQueryLog.getInstance().get(queryId).beginCall(range, ActiveQuery.CallType.NEXT);
                    if (randomSleep) {
                        TimeUnit.SECONDS.sleep(createRandomDelay());
                    }
                    ActiveQueryLog.getInstance().get(queryId).endCall(range, ActiveQuery.CallType.NEXT);
                    qs.next();
                    qs.next();
                }
                Document d = new Document();
                d.put("TEST1", new PreNormalizedAttribute("value", null, true));
                d.put("TEST2", new PreNormalizedAttribute("value", null, true));
                ActiveQueryLog.getInstance().get(queryId).recordStats(d, qs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    // Verify the default instance is returned for a null name.
    @Test
    public void testGetInstanceWithNullName() {
        ActiveQueryLog namedInstance = ActiveQueryLog.getInstance(null);
        assertSame(namedInstance, ActiveQueryLog.getInstance());
    }
    
    // Verify the default instance is returned for a blank name.
    @Test
    public void testGetInstanceWithBlankName() {
        ActiveQueryLog namedInstance = ActiveQueryLog.getInstance(" ");
        assertSame(namedInstance, ActiveQueryLog.getInstance());
    }
    
    // Verify a new instance is returned for a new name.
    @Test
    public void testGetInstanceWithNewName() {
        ActiveQueryLog namedInstance = ActiveQueryLog.getInstance("name");
        assertNotNull(namedInstance);
        assertNotSame(namedInstance, ActiveQueryLog.getInstance());
    }
    
    // Verify the associated existing instance is returned for an existing name.
    @Test
    public void testGetInstanceWithExistingName() {
        ActiveQueryLog firstInstance = ActiveQueryLog.getInstance("test");
        ActiveQueryLog secondInstance = ActiveQueryLog.getInstance("test");
        assertSame(firstInstance, secondInstance);
        assertNotSame(secondInstance, ActiveQueryLog.getInstance());
    }
    
    @Test
    public void testThreadSafety() {
        
        ActiveQueryLog.getInstance().setLogPeriod(2000);
        
        int numQueries = 10;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numQueries);
        
        Map<String,QueryTask> tasks = new HashMap<>();
        for (int i = 1; i <= numQueries; i++) {
            String queryId = createQueryId();
            int numSeeks = rand.nextInt(5) + 1;
            int numNexts = rand.nextInt(20) + 1;
            QueryTask task = new QueryTask(queryId, numSeeks, numNexts, false);
            tasks.put(queryId, task);
            executor.execute(task);
        }
        executor.shutdown();
        while (executor.getCompletedTaskCount() < numQueries) {
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException ignored) {}
        }
        
        for (Map.Entry<String,QueryTask> e : tasks.entrySet()) {
            ActiveQuery activeQuery = ActiveQueryLog.getInstance().get(e.getKey());
            QueryTask queryTask = e.getValue();
            ActiveQuerySnapshot snapshot = activeQuery.snapshot();
            Assert.assertEquals(queryTask.numSeeks, snapshot.getNumCalls(ActiveQuery.CallType.SEEK));
            Assert.assertEquals(queryTask.numNexts, snapshot.getNumCalls(ActiveQuery.CallType.NEXT));
        }
    }
}
