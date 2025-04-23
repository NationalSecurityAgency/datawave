package datawave.metrics;

import static com.codahale.metrics.MetricRegistry.name;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/**
 * Reports Dropwizard metrics to StatsD.
 */
public class StatsDMetricReporter extends ScheduledReporter {
    private static final Logger LOG = LoggerFactory.getLogger(StatsDMetricReporter.class);
    private final StatsDClient statsDClient;
    
    protected StatsDMetricReporter(MetricRegistry registry, StatsDClient statsDClient, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, "statsd-reporter", filter, rateUnit, durationUnit);
        this.statsDClient = statsDClient;
    }
    
    @Override
    public void report(SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters, SortedMap<String,Histogram> histograms,
                    SortedMap<String,Meter> meters, SortedMap<String,Timer> timers) {
        for (Entry<String,Gauge> entry : gauges.entrySet()) {
            reportGauge(entry.getKey(), entry.getValue());
        }
        
        for (Entry<String,Counter> entry : counters.entrySet()) {
            reportCounter(entry.getKey(), entry.getValue());
        }
        
        for (Entry<String,Histogram> entry : histograms.entrySet()) {
            reportHistogram(entry.getKey(), entry.getValue());
        }
        
        for (Entry<String,Meter> entry : meters.entrySet()) {
            reportMeter(entry.getKey(), entry.getValue());
        }
        
        for (Entry<String,Timer> entry : timers.entrySet()) {
            reportTimer(entry.getKey(), entry.getValue());
        }
    }
    
    private void reportGauge(String name, Gauge<?> gauge) {
        Object value = gauge.getValue();
        if (value != null) {
            if (value instanceof Float || value instanceof Double) {
                statsDClient.gauge(name, ((Number) value).doubleValue());
            } else if (value instanceof Number) {
                statsDClient.gauge(name, ((Number) value).longValue());
            } else {
                statsDClient.gauge(name, String.valueOf(value));
            }
        }
    }
    
    private void reportCounter(String name, Counter value) {
        statsDClient.count(name, value.getCount());
    }
    
    private void reportHistogram(String name, Histogram histogram) {
        Snapshot snapshot = histogram.getSnapshot();
        statsDClient.gauge(name(name, "count"), histogram.getCount());
        statsDClient.gauge(name(name, "max"), snapshot.getMax());
        statsDClient.gauge(name(name, "mean"), snapshot.getMean());
        statsDClient.gauge(name(name, "min"), snapshot.getMin());
        statsDClient.gauge(name(name, "stddev"), snapshot.getStdDev());
        statsDClient.gauge(name(name, "median"), snapshot.getMedian());
        statsDClient.gauge(name(name, "p75"), snapshot.get75thPercentile());
        statsDClient.gauge(name(name, "p95"), snapshot.get95thPercentile());
        statsDClient.gauge(name(name, "p98"), snapshot.get98thPercentile());
        statsDClient.gauge(name(name, "p99"), snapshot.get99thPercentile());
        statsDClient.gauge(name(name, "p999"), snapshot.get999thPercentile());
    }
    
    private void reportMeter(String name, Metered meter) {
        statsDClient.gauge(name(name, "count"), meter.getCount());
        statsDClient.gauge(name(name, "m1_rate"), convertRate(meter.getOneMinuteRate()));
        statsDClient.gauge(name(name, "m5_rate"), convertRate(meter.getFiveMinuteRate()));
        statsDClient.gauge(name(name, "m15_rate"), convertRate(meter.getFifteenMinuteRate()));
        statsDClient.gauge(name(name, "mean_rate"), convertRate(meter.getMeanRate()));
    }
    
    private void reportTimer(String name, Timer timer) {
        Snapshot snapshot = timer.getSnapshot();
        statsDClient.gauge(name(name, "max"), convertDuration(snapshot.getMax()));
        statsDClient.gauge(name(name, "mean"), convertDuration(snapshot.getMean()));
        statsDClient.gauge(name(name, "min"), convertDuration(snapshot.getMin()));
        statsDClient.gauge(name(name, "stddev"), convertDuration(snapshot.getStdDev()));
        statsDClient.gauge(name(name, "median"), convertDuration(snapshot.getMedian()));
        statsDClient.gauge(name(name, "p75"), convertDuration(snapshot.get75thPercentile()));
        statsDClient.gauge(name(name, "p95"), convertDuration(snapshot.get95thPercentile()));
        statsDClient.gauge(name(name, "p98"), convertDuration(snapshot.get98thPercentile()));
        statsDClient.gauge(name(name, "p99"), convertDuration(snapshot.get99thPercentile()));
        statsDClient.gauge(name(name, "p999"), convertDuration(snapshot.get999thPercentile()));
        
        reportMeter(name, timer);
    }
    
    @Override
    public void stop() {
        super.stop();
        try {
            statsDClient.stop();
        } catch (Exception e) {
            LOG.warn("Unable to shut down StatsD client: {}", e.getMessage(), e);
        }
    }
}
