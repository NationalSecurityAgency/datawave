package datawave.metrics.cdi;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

/**
 * The single Dropwizard {@link MetricRegistry} that the web service metrics are recorded in and reported from.
 * <p>
 * The registry is held in {@link SharedMetricRegistries} rather than only as a CDI bean so that every module of the EAR resolves the same instance: the EAR
 * library jars cannot see beans defined in the EJB modules, but they all share the metrics-core classes in the EAR's lib directory.
 */
public final class DatawaveMetricRegistry {

    public static final String NAME = "datawave";

    private DatawaveMetricRegistry() {}

    public static MetricRegistry get() {
        return SharedMetricRegistries.getOrCreate(NAME);
    }
}
