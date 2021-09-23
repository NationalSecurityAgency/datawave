package datawave.query.metrics;

import datawave.services.query.metric.QueryMetricHandler;
import datawave.webservice.query.metric.QueryMetric;

/**
 * When instantiated via PowerMock through a test instance of MockShardTableQueryMetricHandler, creates a mock instance of QueryMetricHandler. Otherwise,
 * creates a null QueryMetricHandler.
 */
public class MockQueryMetricHandlerFactory {
    public QueryMetricHandler<QueryMetric> newMockQueryMetricHandler() {
        return null;
    }
}
