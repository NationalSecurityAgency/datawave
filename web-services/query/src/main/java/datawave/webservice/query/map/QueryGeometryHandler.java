package datawave.webservice.query.map;

import java.util.List;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryGeometryResponse;

public interface QueryGeometryHandler {

    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
