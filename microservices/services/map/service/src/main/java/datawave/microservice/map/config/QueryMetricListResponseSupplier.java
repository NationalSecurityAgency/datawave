package datawave.microservice.map.config;

import java.util.function.Supplier;

import org.springframework.stereotype.Component;

import datawave.microservice.querymetric.BaseQueryMetricListResponse;
import datawave.microservice.querymetric.QueryMetricListResponse;

@Component
public class QueryMetricListResponseSupplier implements Supplier<BaseQueryMetricListResponse> {
    @Override
    public BaseQueryMetricListResponse get() {
        return new QueryMetricListResponse();
    }
}
