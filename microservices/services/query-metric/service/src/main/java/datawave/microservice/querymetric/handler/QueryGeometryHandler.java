package datawave.microservice.querymetric.handler;

import java.util.List;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryGeometryResponse;
import datawave.microservice.querymetric.factory.QueryMetricResponseFactory;

public interface QueryGeometryHandler {
    
    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
    
    void setQueryMetricResponseFactory(QueryMetricResponseFactory queryMetricResponseFactory);
}
