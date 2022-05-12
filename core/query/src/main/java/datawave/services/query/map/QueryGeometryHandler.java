package datawave.services.query.map;

import datawave.webservice.query.map.QueryGeometryResponse;
import datawave.microservice.querymetric.BaseQueryMetric;

import java.util.List;

public interface QueryGeometryHandler {
    
    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
