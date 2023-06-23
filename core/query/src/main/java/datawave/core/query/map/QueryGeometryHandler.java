package datawave.core.query.map;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.map.QueryGeometryResponse;

import java.util.List;

public interface QueryGeometryHandler {

    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
