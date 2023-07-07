package datawave.webservice.query.map;

import java.util.List;

import datawave.microservice.querymetric.BaseQueryMetric;

public interface QueryGeometryHandler {

    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
