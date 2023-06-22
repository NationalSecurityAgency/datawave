package datawave.webservice.query.map;

import datawave.microservice.querymetric.BaseQueryMetric;

import java.util.List;

public interface QueryGeometryHandler {

    QueryGeometryResponse getQueryGeometryResponse(String id, List<? extends BaseQueryMetric> queries);
}
