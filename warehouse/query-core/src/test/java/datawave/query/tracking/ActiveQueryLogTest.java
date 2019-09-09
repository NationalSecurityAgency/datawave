package datawave.query.tracking;

import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.iterator.profile.QuerySpan;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ActiveQueryLogTest {
    
    private static Random rand = new Random();
    
    private static String createQueryId() {
        return (RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils.randomAlphanumeric(5) + "-" + RandomStringUtils
                        .randomAlphanumeric(5)).toUpperCase();
    }
    
    private static long createRandomDelay() {
        return rand.nextInt(3) + 1;
    }
    
    public class QueryTask implements Runnable {
        private String queryId;
        private int numSeeks;
        private int numNexts;
        private boolean randomSleep;
        
        public QueryTask(String queryId, int numSeeks, int numNexts, boolean randomSleep) {
            this.queryId = queryId;
            this.numSeeks = numSeeks;
            this.numNexts = numNexts;
            this.randomSleep = randomSleep;
        }
        
        public void run() {
            try {
                QuerySpan qs = new QuerySpan(null);
                for (int seek = 0; seek < numSeeks; seek++) {
                    ActiveQueryLog.getInstance().get(queryId).beginCall(ActiveQuery.CallType.SEEK);
                    if (randomSleep) {
                        TimeUnit.SECONDS.sleep(createRandomDelay());
                    }
                    ActiveQueryLog.getInstance().get(queryId).endCall(ActiveQuery.CallType.SEEK);
                    qs.seek();
                    qs.seek();
                }
                
                for (int next = 0; next < numNexts; next++) {
                    ActiveQueryLog.getInstance().get(queryId).beginCall(ActiveQuery.CallType.NEXT);
                    if (randomSleep) {
                        TimeUnit.SECONDS.sleep(createRandomDelay());
                    }
                    ActiveQueryLog.getInstance().get(queryId).endCall(ActiveQuery.CallType.NEXT);
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
    
    @Test
    public void testThreadSafety() {
        
        int numQueries = 20;
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
            } catch (InterruptedException e) {
                
            }
        }
        
        for (Map.Entry<String,QueryTask> e : tasks.entrySet()) {
            ActiveQuery activeQuery = ActiveQueryLog.getInstance().get(e.getKey());
            QueryTask queryTask = e.getValue();
            Assert.assertEquals(queryTask.numSeeks, activeQuery.getNumCalls(ActiveQuery.CallType.SEEK));
            Assert.assertEquals(queryTask.numNexts, activeQuery.getNumCalls(ActiveQuery.CallType.NEXT));
        }
    }
}
