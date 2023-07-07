package datawave.query.metrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetric;
import datawave.microservice.querymetric.QueryMetricsSummaryResponse;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.security.authorization.DatawavePrincipal;

public class BaseQueryMetricHandlerTest {

    private TestQueryMetricHandler queryMetricHandler;

    @Before
    public void setup() {
        this.queryMetricHandler = new TestQueryMetricHandler();
    }

    @Test
    public void testPopulateMetricSelectors() {

        LuceneToJexlQueryParser parser = new LuceneToJexlQueryParser();
        QueryMetric metric = new QueryMetric();
        metric.setQueryType("RunningQuery");
        metric.setLifecycle(BaseQueryMetric.Lifecycle.DEFINED);
        metric.setQuery("FIELD:value AND #UNKNOWNFUNCTION(parameter)");
        this.queryMetricHandler.populateMetricSelectors(metric, parser);
        Assert.assertNull(metric.getPositiveSelectors());

        metric.setQuery("FIELD1:value1 NOT FIELD2:value2 AND #ISNOTNULL(OTHER)");
        this.queryMetricHandler.populateMetricSelectors(metric, parser);
        Assert.assertEquals(1, metric.getPositiveSelectors().size());
        Assert.assertEquals("FIELD1:value1", metric.getPositiveSelectors().get(0));
        Assert.assertEquals(1, metric.getNegativeSelectors().size());
        Assert.assertEquals("FIELD2:value2", metric.getNegativeSelectors().get(0));
    }

    @Test
    public void testNumUpdates() {
        QueryMetric metric = new QueryMetric();
        this.queryMetricHandler.incrementNumUpdates(metric, Collections.singleton(metric));
        Assert.assertEquals(1, metric.getNumUpdates());

        metric.setNumUpdates(0);
        List<QueryMetric> metricList = new ArrayList<>();
        QueryMetric metric1 = new QueryMetric();
        metric1.setNumUpdates(2);
        metricList.add(metric1);
        QueryMetric metric2 = new QueryMetric();
        metric2.setNumUpdates(8);
        metricList.add(metric2);
        QueryMetric metric3 = new QueryMetric();
        metric3.setNumUpdates(5);
        metricList.add(metric3);
        this.queryMetricHandler.incrementNumUpdates(metric, metricList);
        Assert.assertEquals(9, metric.getNumUpdates());
    }

    private static class TestQueryMetricHandler extends BaseQueryMetricHandler<QueryMetric> {

        @Override
        public void updateMetric(QueryMetric metric, DatawavePrincipal datawavePrincipal) throws Exception {

        }

        @Override
        public Map<String,String> getEventFields(BaseQueryMetric queryMetric) {
            return null;
        }

        @Override
        public BaseQueryMetricListResponse<QueryMetric> query(String user, String queryId, DatawavePrincipal datawavePrincipal) {
            return null;
        }

        @Override
        public BaseQueryMetricListResponse<QueryMetric> subplan(String user, String queryId, DatawavePrincipal datawavePrincipal) {
            return null;
        }

        @Override
        public QueryMetricsSummaryResponse getTotalQueriesSummaryCounts(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
            return null;
        }

        @Override
        public QueryMetricsSummaryResponse getTotalQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
            return null;
        }

        @Override
        public QueryMetricsSummaryResponse getUserQueriesSummary(Date begin, Date end, DatawavePrincipal datawavePrincipal) {
            return null;
        }

        @Override
        public void flush() throws Exception {

        }

        @Override
        public void reload() {

        }
    }
}
