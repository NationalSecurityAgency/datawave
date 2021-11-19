package datawave.microservice.query.messaging;

/**
 * This callback is used to ack/nack messages
 */
public interface AcknowledgementCallback {
    void acknowledge(Status status);
    
    enum Status {
        ACK, NACK
    }
}
