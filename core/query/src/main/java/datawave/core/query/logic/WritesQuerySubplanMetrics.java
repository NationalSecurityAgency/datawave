package datawave.core.query.logic;

import datawave.microservice.querymetric.BaseQueryMetric;

public interface WritesQuerySubplanMetrics {

    /**
     * This will set the query metric associated with the query logic.
     *
     * @param metric
     *            The query metric
     */
    void setQueryMetric(BaseQueryMetric metric);

    /**
     * This will allow us to get the metrics associated with a query logic.
     *
     * @return query metric
     */
    BaseQueryMetric getQueryMetric();
}
