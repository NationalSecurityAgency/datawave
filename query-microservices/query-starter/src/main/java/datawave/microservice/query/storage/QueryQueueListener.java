package datawave.microservice.query.storage;

import org.springframework.messaging.Message;

public interface QueryQueueListener {
    long WAIT_MS = 60 * 1000L;
    
    /**
     * Get the listener id
     * 
     * @return the listener id
     */
    String getListenerId();
    
    /**
     * Receive a message
     * 
     * @return the message, or null if none available within WAIT_MS
     */
    default Message<Result> receive() {
        return receive(WAIT_MS);
    }
    
    /**
     * Receive a message
     * 
     * @param waitMs
     *            The milliseconds to wait for an available result
     * @return the message, or null if none available within waitMs
     */
    Message<Result> receive(long waitMs);
    
    /**
     * Do we have any results pending
     * 
     * @return true if we have results, false if none pending (yet as they could be in transit)
     */
    boolean hasResults();
    
    /**
     * Stop the listener, effectively destroying the listener
     */
    void stop();
}
