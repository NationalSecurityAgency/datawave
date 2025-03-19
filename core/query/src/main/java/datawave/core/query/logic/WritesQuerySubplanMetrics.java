package datawave.core.query.logic;

import java.util.Map;

import datawave.microservice.querymetric.RangeCounts;

public interface WritesQuerySubplanMetrics {

    /**
     * This will set the query metric associated with the query logic.
     *
     * @param metric
     *            The query metric
     */
    // void setQueryMetric(BaseQueryMetric metric);

    /**
     * This will allow us to get the metrics associated with a query logic.
     *
     * @return query metric
     */
    // BaseQueryMetric getQueryMetric();

    Map<String,RangeCounts> getSubPlans();

    void setSubPlans(Map<String,RangeCounts> subPlans);

    void addSubPlan(String newQuery, RangeCounts ranges);
}
