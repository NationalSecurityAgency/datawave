package datawave.metrics;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricFactoryImpl;

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
