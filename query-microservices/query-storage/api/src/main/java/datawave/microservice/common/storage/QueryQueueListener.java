package datawave.microservice.common.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.messaging.Message;

import java.io.IOException;

public interface QueryQueueListener {
    long WAIT_MS = 100L;
    
    /**
     * Get the listener id
     * 
     * @return the listener id
     */
    String getListenerId();
    
    /**
     * Receive a query task notification
     * 
     * @return the query task notification, or null if none available within WAIT_MS
     * @throws IOException
     *             if the message could not be deserialized into a QueryTaskNotification
     */
    default QueryTaskNotification receiveTaskNotification() throws IOException {
        return receiveTaskNotification(100L);
    }
    
    /**
     * Receive a query task notification
     * 
     * @param waitMs
     *            The time to wait
     * @return the query task notification, or null if none available with the specified waitMs
     * @throws IOException
     *             if the message could not be deserialized into a QueryTaskNotification
     */
    default QueryTaskNotification receiveTaskNotification(long waitMs) throws IOException {
        Message<byte[]> message = receive(waitMs);
        if (message != null) {
            return new ObjectMapper().readerFor(QueryTaskNotification.class).readValue(message.getPayload());
        }
        return null;
    }
    
    /**
     * Receive a message
     * 
     * @return the message, or null if none available within WAIT_MS
     */
    default Message<byte[]> receive() {
        return receive(WAIT_MS);
    }
    
    /**
     * Receive a message
     * 
     * @param waitMs
     * @return the message, or null if none available within waitMs
     */
    Message<byte[]> receive(long waitMs);
    
    /**
     * Stop the listener, effectively destroying the listener
     */
    void stop();
}
