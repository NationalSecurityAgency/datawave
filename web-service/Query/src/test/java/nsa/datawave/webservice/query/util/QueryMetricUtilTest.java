package nsa.datawave.webservice.query.util;

import java.util.UUID;

import nsa.datawave.security.util.DnUtils.NpeUtils;
import nsa.datawave.webservice.query.metric.QueryMetric;
import nsa.datawave.webservice.query.metric.BaseQueryMetric.PageMetric;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryMetricUtilTest {
    
    private QueryMetric metric = null;
    
    private String sid = "me";
    private String queryId = UUID.randomUUID().toString();
    private Class<?> queryType = QueryMetricUtilTest.class;
    private long setupTime = 100L;
    private PageMetric page1 = new PageMetric(50, 150, 0, 0);
    private PageMetric page2 = new PageMetric(25, 75, 0, 0);
    
    @Before
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
        Assert.assertEquals(metric, metric2);
    }
    
}
