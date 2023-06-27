package datawave.core.query.map;

import java.util.List;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.webservice.query.map.QueryGeometryResponse;

public interface QueryGeometryHandler {

    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
