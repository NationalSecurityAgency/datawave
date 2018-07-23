package datawave.microservice.config.metrics;

import com.codahale.metrics.ScheduledReporter;
import datawave.metrics.MetricsReporterFactory;
import datawave.metrics.NoOpMetricsReporterFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

/**
 * {@link ConfigurationProperties} for a dropwizard {@link ScheduledReporter}.
 */
@ConfigurationProperties(prefix = "metrics.reporter")
public class MetricsConfigurationProperties {
    private Class<? extends MetricsReporterFactory> factoryClass = NoOpMetricsReporterFactory.class;
    private String host = "localhost";
    private int port = 54321;
    private long interval = 30L;
    private TimeUnit intervalUnit = TimeUnit.SECONDS;
    
    public Class<? extends MetricsReporterFactory> getFactoryClass() {
        return factoryClass;
    }
    
    public void setFactoryClass(Class<? extends MetricsReporterFactory> factoryClass) {
        this.factoryClass = factoryClass;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public int getPort() {
        return port;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public long getInterval() {
        return interval;
    }
    
    public void setInterval(long interval) {
        this.interval = interval;
    }
    
    public TimeUnit getIntervalUnit() {
        return intervalUnit;
    }
    
    public void setIntervalUnit(TimeUnit intervalUnit) {
        this.intervalUnit = intervalUnit;
    }
}
