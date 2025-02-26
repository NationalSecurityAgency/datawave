package datawave.metrics;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

public class NoOpMetricsReporter extends ScheduledReporter {
    
    protected NoOpMetricsReporter(MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
    }
    
    @Override
    public void report(SortedMap<String,Gauge> sortedMap, SortedMap<String,Counter> sortedMap1, SortedMap<String,Histogram> sortedMap2,
                    SortedMap<String,Meter> sortedMap3, SortedMap<String,Timer> sortedMap4) {
        
    }
}
