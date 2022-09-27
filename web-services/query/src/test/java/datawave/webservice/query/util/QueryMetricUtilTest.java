package datawave.webservice.query.util;

import datawave.microservice.querymetric.BaseQueryMetric.PageMetric;
import datawave.microservice.querymetric.QueryMetric;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

public class QueryMetricUtilTest {
    
    private QueryMetric metric = null;
    
    private String sid = "me";
    private String queryId = UUID.randomUUID().toString();
    private Class<?> queryType = QueryMetricUtilTest.class;
    private long setupTime = 100L;
    private PageMetric page1 = new PageMetric("localhost", 50, 150, 0, 0, -1, -1, -1, -1);
    private PageMetric page2 = new PageMetric("localhost", 25, 75, 0, 0, -1, -1, -1, -1);
    
    @BeforeEach
    public void setup() {
        metric = new QueryMetric();
        metric.setQueryId(queryId);
        metric.setQueryType(queryType);
        metric.setSetupTime(setupTime);
        metric.setUser(sid);
        metric.getPageTimes().add(page1);
        metric.getPageTimes().add(page2);
    }
    
    @Test
    public void testSerialization() throws Exception {
        Mutation m = QueryMetricUtil.toMutation(metric);
        QueryMetric metric2 = (QueryMetric) QueryMetricUtil.toMetric(new Value(m.getUpdates().get(0).getValue()));
        Assertions.assertEquals(metric, metric2);
    }
    
}
