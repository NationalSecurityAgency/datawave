package datawave.util.time;

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Utility for measuring the time taken to perform some operation.
 */
public class TraceStopwatch {
    
    protected final String description;
    protected final Stopwatch sw;
    protected Span span;
    
    public TraceStopwatch(String description) {
        Preconditions.checkNotNull(description);
        
        this.description = description;
        this.sw = Stopwatch.createUnstarted();
    }
    
    public String description() {
        return this.description;
    }
    
    public boolean isRunning() {
        // Wild on you
        return this.sw.isRunning();
    }
    
    public void start() {
        span = Trace.start(description);
        this.sw.start();
    }
    
    public void data(String name, String value) {
        span.data(name, value);
    }
    
    public void stop() {
        this.sw.stop();
        
        if (null != span) {
            span.stop();
        }
    }
    
    public long elapsed(TimeUnit desiredUnit) {
        return sw.elapsed(desiredUnit);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(span.hashCode()).append(description).append(sw.hashCode()).toHashCode();
    }
    
    @Override
    public String toString() {
        return sw.toString();
    }
}
