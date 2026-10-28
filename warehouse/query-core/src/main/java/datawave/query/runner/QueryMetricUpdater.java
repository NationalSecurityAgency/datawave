package datawave.query.runner;

import datawave.microservice.querymetric.BaseQueryMetric;

public interface QueryMetricUpdater {

    void updateMetric(BaseQueryMetric metric) throws Exception;
}
