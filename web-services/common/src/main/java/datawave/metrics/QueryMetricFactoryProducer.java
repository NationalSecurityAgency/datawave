package datawave.metrics;

import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

/**
 * A CDI producer class whose purposes is to produce an implementation of {@link QueryMetricFactory}. The default implementation is not in a library that is
 * marked as a bean archive, so it must be explicitly produced.
 */
@ApplicationScoped
public class QueryMetricFactoryProducer {
    @Produces
    public QueryMetricFactory queryMetricFactory() {
        return new QueryMetricFactoryImpl();
    }
}
