package datawave.metrics.cdi;

import java.lang.annotation.Annotation;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Metric;

import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;

/**
 * Produces the {@link Counter} for an {@code @Inject @Metric Counter} injection point from the {@link DatawaveMetricRegistry}. The counter is named the way
 * metrics-cdi did: the annotation's name when it is absolute, otherwise the name (or the field name) prefixed with the declaring class name.
 */
public class MetricProducer {

    @Produces
    @Metric
    public Counter counter(InjectionPoint ip) {
        return DatawaveMetricRegistry.get().counter(metricName(ip));
    }

    static String metricName(InjectionPoint ip) {
        Metric metric = null;
        for (Annotation qualifier : ip.getQualifiers()) {
            if (qualifier instanceof Metric) {
                metric = (Metric) qualifier;
                break;
            }
        }
        String name = (metric == null || metric.name().isEmpty()) ? ip.getMember().getName() : metric.name();
        if (metric != null && metric.absolute()) {
            return name;
        }
        return MetricRegistry.name(ip.getMember().getDeclaringClass(), name);
    }
}
