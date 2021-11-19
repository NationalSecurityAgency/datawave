package datawave.microservice.query.messaging;

import java.util.concurrent.TimeUnit;

public interface QueryResultsPublisher {
    
    /**
     * Publishes a result for the query
     * 
     * @param result
     *            the result to publish
     * @return true if successful, false otherwise
     */
    default boolean publish(Result result) {
        return publish(result, Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Publishes a result for the query
     * 
     * @param result
     *            the result to publish
     * @param interval
     *            the amount of time to wait for the publish response
     * @param timeUnit
     *            the time unit
     * @return true if successful, false otherwise
     */
    boolean publish(Result result, long interval, TimeUnit timeUnit);
}
