package datawave.webservice.query.map;

import datawave.webservice.query.metric.BaseQueryMetric;

import java.util.List;

public interface QueryGeometryHandler {
    
    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
