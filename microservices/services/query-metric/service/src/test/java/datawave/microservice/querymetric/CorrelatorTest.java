package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.microservice.querymetric.config.CorrelatorProperties;
import datawave.microservice.querymetric.function.QueryMetricConsumer;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"CorrelatorTest", "QueryMetricTest", "hazelcast-writebehind", "correlator"})
public class CorrelatorTest extends QueryMetricTestBase {
    
    @Autowired
    private QueryMetricOperations queryMetricOperations;
    
    @Autowired
    private CorrelatorProperties correlatorProperties;
    
    @Autowired
    private Correlator correlator;
    
    @Autowired
    private QueryMetricConsumer queryMetricConsumer;
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
        this.correlator.shutdown(false);
    }
    
    @Test
    public void TestSizeLimitedQueue() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(30000);
        this.correlatorProperties.setMaxCorrelationQueueSize(95);
        testMetricsCorrelated(100, 100);
    }
    
    @Test
    public void TestTimeLimitedQueue() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(500);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(100, 100);
    }
    
    @Test
    public void TestNoStorageUntilShutdown() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(60000);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(100, 10);
    }
    
    @Test
    public void TestManyQueries() throws Exception {
        this.correlatorProperties.setMaxCorrelationTimeMs(500);
        this.correlatorProperties.setMaxCorrelationQueueSize(100);
        testMetricsCorrelated(1000, 20);
    }
    
    public void testMetricsCorrelated(int numMetrics, int maxPages) throws Exception {
        List<BaseQueryMetric> updates = new ArrayList<>();
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int x = 0; x < numMetrics; x++) {
            BaseQueryMetric m = createMetric();
            metrics.add(m);
            updates.add(m.duplicate());
        }
        
        List<BaseQueryMetric> shuffledUpdates = new ArrayList<>();
        Random r = new Random();
        for (BaseQueryMetric m : metrics) {
            int numPages = r.nextInt(maxPages);
            BaseQueryMetric m2 = m;
            for (int x = 0; x < numPages; x++) {
                m2 = m2.duplicate();
                m.setLifecycle(BaseQueryMetric.Lifecycle.RESULTS);
                m2.setLifecycle(BaseQueryMetric.Lifecycle.RESULTS);
                BaseQueryMetric.PageMetric pageMetric = new BaseQueryMetric.PageMetric("localhost", 100, 100, 100, 100, -1, -1, -1, -1);
                m.addPageMetric(pageMetric);
                m2.addPageMetric(pageMetric);
                shuffledUpdates.add(m2);
            }
        }
        
        // randomize the order of metric updates
        Collections.shuffle(shuffledUpdates);
        updates.addAll(shuffledUpdates);
        
        LinkedBlockingDeque<BaseQueryMetric> updateDeque = new LinkedBlockingDeque<>();
        updateDeque.addAll(updates);
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int x = 0; x < 20; x++) {
            Runnable runnable = () -> {
                BaseQueryMetric m;
                do {
                    m = updateDeque.poll();
                    if (m != null) {
                        queryMetricConsumer.accept(new QueryMetricUpdate(m, QueryMetricType.COMPLETE));
                    }
                } while (m != null);
            };
            executorService.submit(runnable);
        }
        log.debug("done submitting metrics");
        executorService.shutdown();
        boolean completed = executorService.awaitTermination(2, TimeUnit.MINUTES);
        assertTrue(completed, "executor tasks completed");
        // flush the correlator
        this.correlator.shutdown(true);
        while (this.queryMetricOperations.isTimedCorrelationInProgress()) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                
            }
        }
        this.queryMetricOperations.ensureUpdatesProcessed(false);
        for (BaseQueryMetric m : metrics) {
            String queryId = m.getQueryId();
            ensureDataStored(incomingQueryMetricsCache, queryId);
            QueryMetricUpdate metricUpdate = incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class);
            assertNotNull(metricUpdate, "missing metric " + queryId);
            assertEquals(m, metricUpdate.getMetric(), "incomingQueryMetricsCache metric wrong for id:" + m.getQueryId());
        }
    }
}
