package datawave.metrics.cdi;

import com.codahale.metrics.annotation.Metric;
import com.codahale.metrics.annotation.Timed;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.util.Nonbinding;

/**
 * Gives the Dropwizard metrics annotations their CDI meaning, which the metrics-cdi library used to provide (metrics-cdi has no Jakarta EE release):
 * <ul>
 * <li>{@link Timed} becomes an interceptor binding, handled by {@link TimedInterceptor}</li>
 * <li>{@link Metric} becomes a qualifier, so that {@code @Inject @Metric Counter} injection points are satisfied by {@link MetricProducer}</li>
 * </ul>
 * All annotation members are made non-binding: the name and the absolute flag select the metric, not the interceptor or the producer.
 */
public class MetricsAnnotationExtension implements Extension {

    void registerMetricsAnnotations(@Observes BeforeBeanDiscovery bbd) {
        bbd.configureInterceptorBinding(Timed.class).methods().forEach(m -> m.add(Nonbinding.Literal.INSTANCE));
        bbd.configureQualifier(Metric.class).methods().forEach(m -> m.add(Nonbinding.Literal.INSTANCE));
    }
}
