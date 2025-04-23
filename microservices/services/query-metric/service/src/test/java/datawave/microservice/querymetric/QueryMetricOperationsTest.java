package datawave.microservice.querymetric;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.microservice.querymetric.config.QueryMetricTransportType;

public abstract class QueryMetricOperationsTest extends QueryMetricTestBase {
    
    @BeforeEach
    public void setup() {
        super.setup();
    }
    
    @AfterEach
    public void cleanup() {
        super.cleanup();
    }
    
    @Test
    public void MetricStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        String queryId = createQueryId();
        BaseQueryMetric m = createMetric(queryId);
        long created = m.getCreateDate().getTime();
        m.addPageTime("localhost", 1000, 1000, created - 1000, created);
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetric(m)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build(), QueryMetricTransportType.MESSAGE);
        // @formatter:on
        ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
        assertNotNull(lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class),
                        "no query exists in lastWrittenQueryMetricCache with that queryId");
        metricAssertEquals("lastWrittenQueryMetricCache metric wrong", m, lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
        metricAssertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
        metricAssertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
    }
    
    @Test
    public void MultipleMetricsStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
            // @formatter:off
            client.submit(new QueryMetricClient.Request.Builder()
                    .withMetric(m)
                    .withMetricType(QueryMetricType.COMPLETE)
                    .withUser(adminUser)
                    .build(), QueryMetricTransportType.MESSAGE);
            // @formatter:on
        }
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
            assertNotNull(lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class),
                            "no query exists in lastWrittenQueryMetricCache with that queryId");
            metricAssertEquals("lastWrittenQueryMetricCache metric wrong", m,
                            lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
            try {
                metricAssertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                fail(e.getMessage());
            }
            metricAssertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
        });
    }
    
    @Test
    public void MultipleMetricsAsListStoredCorrectlyInCachesAndAccumulo() throws Exception {
        
        List<BaseQueryMetric> metrics = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String id = createQueryId();
            BaseQueryMetric m = createMetric(id);
            metrics.add(m);
        }
        // @formatter:off
        client.submit(new QueryMetricClient.Request.Builder()
                .withMetrics(metrics)
                .withMetricType(QueryMetricType.COMPLETE)
                .withUser(adminUser)
                .build(), QueryMetricTransportType.MESSAGE);
        // @formatter:on
        metrics.forEach((m) -> {
            String queryId = m.getQueryId();
            ensureDataWritten(incomingQueryMetricsCache, lastWrittenQueryMetricCache, queryId);
            assertNotNull(lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class),
                            "no query exists in lastWrittenQueryMetricCache with that queryId");
            metricAssertEquals("lastWrittenQueryMetricCache metric wrong", m,
                            lastWrittenQueryMetricCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
            metricAssertEquals("incomingQueryMetricsCache metric wrong", m, incomingQueryMetricsCache.get(queryId, QueryMetricUpdateHolder.class).getMetric());
            try {
                metricAssertEquals("accumulo metric wrong", m, shardTableQueryMetricHandler.getQueryMetric(queryId));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
    }
}
