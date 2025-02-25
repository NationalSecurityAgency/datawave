package datawave.metrics;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

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
 * Reports Dropwizard metrics to Timely.
 */
public class TimelyMetricsReporter extends ScheduledReporter {
    protected static final Pattern RACK_PATTERN = Pattern.compile("[a-zA-Z]\\d+n\\d+");
    protected static final long MAX_BACKOFF = TimeUnit.SECONDS.toMillis(120);
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected final String timelyHost;
    protected final int timelyPort;
    protected final String hostname;
    protected final String rackname;
    protected Socket sock;
    protected PrintWriter out;
    protected long connectTime = 0L;
    protected long backoff = 2000;
    
    protected TimelyMetricsReporter(String timelyHost, int timelyPort, MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit,
                    TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.timelyHost = timelyHost;
        this.timelyPort = timelyPort;
        
        String host = "unknown";
        String rack = "unknown";
        try {
            host = InetAddress.getLocalHost().getCanonicalHostName();
            int idx = host.indexOf('.');
            if (idx >= 0) {
                host = host.substring(0, idx);
            }
            if (RACK_PATTERN.matcher(host).matches()) {
                rack = host.substring(0, host.indexOf('n'));
            }
        } catch (UnknownHostException ignored) {
            // ignore - we just won't report our host name
        }
        
        hostname = host;
        rackname = rack;
    }
    
    @Override
    public void report(SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters, SortedMap<String,Histogram> histograms,
                    SortedMap<String,Meter> meters, SortedMap<String,Timer> timers) {
        final long time = System.currentTimeMillis();
        
        if (!connect()) {
            return;
        }
        
        for (Entry<String,Gauge> entry : gauges.entrySet()) {
            reportGauge(entry.getKey(), entry.getValue(), time);
        }
        
        for (Entry<String,Counter> entry : counters.entrySet()) {
            reportCounter(entry.getKey(), entry.getValue(), time);
        }
        
        for (Entry<String,Histogram> entry : histograms.entrySet()) {
            reportHistogram(entry.getKey(), entry.getValue(), time);
        }
        
        for (Entry<String,Meter> entry : meters.entrySet()) {
            reportMeter(entry.getKey(), entry.getValue(), time);
        }
        
        for (Entry<String,Timer> entry : timers.entrySet()) {
            reportTimer(entry.getKey(), entry.getValue(), time);
        }
        
        flush();
    }
    
    private void reportGauge(String name, Gauge<?> gauge, long time) {
        name = name.replaceAll(" ", "_");
        Object value = gauge.getValue();
        if (value != null) {
            if (value instanceof Float || value instanceof Double) {
                reportMetric(name, "value", ((Number) value).doubleValue(), "GAUGE", null, time);
            } else if (value instanceof Number) {
                reportMetric(name, "value", ((Number) value).longValue(), "GAUGE", null, time);
            } else {
                reportMetric(name, "value", String.valueOf(value), "GAUGE", null, time);
            }
        }
    }
    
    private void reportCounter(String name, Counter value, long time) {
        name = name.replaceAll(" ", "_");
        reportMetric(name, "value", value.getCount(), "COUNTER", null, time);
    }
    
    private void reportHistogram(String name, Histogram histogram, long time) {
        name = name.replaceAll(" ", "_");
        Snapshot snapshot = histogram.getSnapshot();
        reportMetric(name, "count", histogram.getCount(), "COUNTER", null, time);
        reportMetric(name, "max", snapshot.getMax(), "GAUGE", null, time);
        reportMetric(name, "mean", snapshot.getMean(), "GAUGE", null, time);
        reportMetric(name, "min", snapshot.getMin(), "GAUGE", null, time);
        reportMetric(name, "stddev", snapshot.getStdDev(), "GAUGE", null, time);
        reportMetric(name, "median", snapshot.getMedian(), "GAUGE", null, time);
        reportMetric(name, "p75", snapshot.get75thPercentile(), "GAUGE", null, time);
        reportMetric(name, "p95", snapshot.get95thPercentile(), "GAUGE", null, time);
        reportMetric(name, "p98", snapshot.get98thPercentile(), "GAUGE", null, time);
        reportMetric(name, "p99", snapshot.get99thPercentile(), "GAUGE", null, time);
        reportMetric(name, "p999", snapshot.get999thPercentile(), "GAUGE", null, time);
    }
    
    private void reportMeter(String name, Metered meter, long time) {
        name = name.replaceAll(" ", "_");
        reportMetric(name, "count", meter.getCount(), "COUNTER", null, time);
        reportMetric(name, "m1_rate", convertRate(meter.getOneMinuteRate()), "GAUGE", getRateUnit(), time);
        reportMetric(name, "m5_rate", convertRate(meter.getFiveMinuteRate()), "GAUGE", getRateUnit(), time);
        reportMetric(name, "m15_rate", convertRate(meter.getFifteenMinuteRate()), "GAUGE", getRateUnit(), time);
        reportMetric(name, "mean_rate", convertRate(meter.getMeanRate()), "GAUGE", getRateUnit(), time);
    }
    
    private void reportTimer(String name, Timer timer, long time) {
        name = name.replaceAll(" ", "_");
        Snapshot snapshot = timer.getSnapshot();
        reportMetric(name, "max", convertDuration(snapshot.getMax()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "mean", convertDuration(snapshot.getMean()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "min", convertDuration(snapshot.getMin()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "stddev", convertDuration(snapshot.getStdDev()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "median", convertDuration(snapshot.getMedian()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "p75", convertDuration(snapshot.get75thPercentile()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "p95", convertDuration(snapshot.get95thPercentile()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "p98", convertDuration(snapshot.get98thPercentile()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "p99", convertDuration(snapshot.get99thPercentile()), "GAUGE", getDurationUnit(), time);
        reportMetric(name, "p999", convertDuration(snapshot.get999thPercentile()), "GAUGE", getDurationUnit(), time);
        
        reportMeter(name, timer, time);
    }
    
    protected void reportMetric(String metricName, String sampleName, double value, String sampleType, String units, long time) {
        reportMetric(metricName, sampleName, String.format("%f", value), sampleType, units, time);
    }
    
    protected void reportMetric(String metricName, String sampleName, long value, String sampleType, String units, long time) {
        reportMetric(metricName, sampleName, String.format("%d", value), sampleType, units, time);
    }
    
    protected void reportMetric(String metricName, String sampleName, String value, String sampleType, String units, long time) {
        StringBuilder message = new StringBuilder();
        message.append(String.format("put %s %d %s host=%s rack=%s sample=%s sampleType=%s", metricName, time, value, hostname, rackname, sampleName,
                        sampleType));
        if (units != null) {
            message.append(" units=").append(units);
        }
        message.append("\n");
        
        reportMetric(message.toString());
    }
    
    protected synchronized void reportMetric(String timelyMetric) {
        out.write(timelyMetric);
    }
    
    protected synchronized void flush() {
        if (out != null) {
            out.flush();
        }
    }
    
    protected synchronized boolean connect() {
        boolean connected = true;
        if (sock == null || !sock.isConnected() || out.checkError()) {
            connected = false;
            final long waitTime = (connectTime + backoff) - System.currentTimeMillis();
            if (waitTime <= 0) {
                try {
                    connectTime = System.currentTimeMillis();
                    sock = new Socket(timelyHost, timelyPort);
                    out = new PrintWriter(new OutputStreamWriter(sock.getOutputStream(), UTF_8), false);
                    backoff = 2000;
                    logger.info("Connected to Timely at {}:{}", timelyHost, timelyPort);
                    connected = true;
                } catch (IOException e) {
                    logger.error("Error connecting to Timely at {}:{}. Error: {}", timelyHost, timelyPort, e.getMessage());
                    backoff = Math.min(backoff * 2, MAX_BACKOFF);
                    sock = null;
                    out = null;
                }
            } else {
                logger.warn("Not writing to Timely, waiting {}ms to reconnect.", waitTime);
            }
        }
        return connected;
    }
    
    @Override
    public void stop() {
        try {
            super.stop();
        } finally {
            if (sock != null) {
                if (out != null) {
                    out.close();
                }
                try {
                    sock.close();
                } catch (IOException e) {
                    logger.error("Error closing connection to Timely at {}:{}. Error: {}", timelyHost, timelyPort, e.getMessage());
                }
            }
        }
    }
}
