package datawave.util.time;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Utility for measuring the time taken to perform some operation.
 */
public class TraceStopwatch {
    static private Logger log = LoggerFactory.getLogger(TraceStopwatch.class);
    
    protected final String description;
    protected final Stopwatch sw;
    
    public TraceStopwatch(String description) {
        Preconditions.checkNotNull(description);
        
        this.description = description;
        this.sw = Stopwatch.createUnstarted();
    }
    
    public String description() {
        return this.description;
    }
    
    public boolean isRunning() {
        return this.sw.isRunning();
    }
    
    public void start() {
        if (log.isTraceEnabled()) {
            log.trace("{} - Stopwatch starting. TID: {}", description, Thread.currentThread().getId());
        }
        this.sw.start();
    }
    
    public void data(String name, String value) {
        if (log.isTraceEnabled()) {
            log.trace("{} - K/V: '{}'/'{}' TID: {}", description, name, value, Thread.currentThread().getId());
        }
    }
    
    public void stop() {
        this.sw.stop();
        if (log.isTraceEnabled()) {
            log.trace("{} - Stopwatch stopped. TID: {}", description, Thread.currentThread().getId());
        }
    }
    
    public long elapsed(TimeUnit desiredUnit) {
        return sw.elapsed(desiredUnit);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(description).append(sw.hashCode()).toHashCode();
    }
    
    @Override
    public String toString() {
        return sw.toString();
    }
}
