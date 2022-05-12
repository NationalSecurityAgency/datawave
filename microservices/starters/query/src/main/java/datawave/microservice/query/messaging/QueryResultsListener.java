package datawave.microservice.query.messaging;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public interface QueryResultsListener extends Closeable {
    
    /**
     * Get the listener id
     * 
     * @return the listener id
     */
    String getListenerId();
    
    /**
     * Receive a message
     * 
     * @return the message, or null if none available
     */
    default Result receive() {
        return receive(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Receive a message
     * 
     * @param interval
     *            The time to wait for an available result
     * @param timeUnit
     *            The time unit
     * @return the result, or null if timeout interval is reached
     */
    Result receive(long interval, TimeUnit timeUnit);
    
    /**
     * Do we have any results pending
     * 
     * @return true if we have results, false if none pending (yet as they could be in transit)
     */
    boolean hasResults();
}
