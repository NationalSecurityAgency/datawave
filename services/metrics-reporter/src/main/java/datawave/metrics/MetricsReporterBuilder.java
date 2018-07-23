package datawave.metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;

/**
 * Generic metrics reporter builder.
 */
public abstract class MetricsReporterBuilder {
    protected final MetricRegistry registry;
    protected String prefix;
    protected TimeUnit rateUnit;
    protected TimeUnit durationUnit;
    protected MetricFilter filter;
    
    protected MetricsReporterBuilder(MetricRegistry registry) {
        this.registry = registry;
        prefix = null;
        rateUnit = TimeUnit.SECONDS;
        durationUnit = TimeUnit.MILLISECONDS;
        filter = MetricFilter.ALL;
    }
    
    public MetricsReporterBuilder prefixedWith(String prefix) {
        this.prefix = prefix;
        return this;
    }
    
    public MetricsReporterBuilder convertRatesTo(TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        return this;
    }
    
    public MetricsReporterBuilder convertDurationsTo(TimeUnit durationUnit) {
        this.durationUnit = durationUnit;
        return this;
    }
    
    public MetricsReporterBuilder filter(MetricFilter filter) {
        this.filter = filter;
        return this;
    }
    
    public abstract ScheduledReporter build(String host, int port);
}
